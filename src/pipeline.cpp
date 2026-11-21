// Shopping Sessions: native pipeline.
// Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet
//
// Phases
//   1. parse (all CSVs, parallel, SIMD block scanner). Links are parsed first and a
//      single thread runs union-find while the others parse events and clicks.
//      Each event copy writes a 16-byte dedup key (scattered by event_id hash) and,
//      if valid, a 16-byte payload into a global sequential array.
//   2a. per event_id partition: find duplicate copies, mark losing copies in a bitmap.
//   2b. scan payloads sequentially, skip losers, map user -> person, scatter by person.
//       Clicks are mapped and scattered the same way.
//   3. per person partition: group, sort, sessionize, attribute, aggregate.
//   4. write Parquet.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON)
#include <arm_neon.h>
#endif

#include "parquet.hpp"

// PARSE_LOOKUP (opt-in): resolve persons while parsing (slower on x86)

typedef uint64_t u64;
typedef int64_t i64;
typedef uint32_t u32;
typedef int32_t i32;

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)

static const i64 DAYMS = 86400000LL;
static const i64 GAPMS = 1800000LL;
static const i64 WEEKMS = 604800000LL;
static const u64 LONGBIT = 1ULL << 63;
static const u32 NOREF = 0xFFFFFFFFu;

static double now_s() {
  return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
static double T0;
static bool g_verbose = true;
#ifdef PROF_STEPS
static double g_st[64][16];
#define STEP_BEGIN double _t0 = now_s();
#define STEP(i)                  \
  {                              \
    double _t1 = now_s();        \
    g_st[tid][i] += _t1 - _t0;   \
    _t0 = _t1;                   \
  }
#else
#define STEP_BEGIN
#define STEP(i)
#endif
static void logt(const char* what) {
  if (g_verbose) fprintf(stderr, "[%7.3f] %s\n", now_s() - T0, what);
}

static void* big_alloc(size_t bytes) {
  if (bytes == 0) bytes = 4096;
  void* m = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (m == MAP_FAILED) {
    fprintf(stderr, "mmap %zu failed\n", bytes);
    exit(1);
  }
#ifdef MADV_HUGEPAGE
  madvise(m, bytes, MADV_HUGEPAGE);
#endif
  return m;
}

// ---------------------------------------------------------------- string codes
// ASCII strings of length 1..9 without NUL pack into 63 bits, 7 bits per char, first char
// most significant: numeric order == byte-wise order. Anything else is interned: LONGBIT|id.
static std::mutex g_imu;
static std::unordered_map<std::string, u32> g_imap;
static std::deque<std::string> g_istr;

__attribute__((noinline)) static u64 intern_slow(const char* p, u32 n) {
  std::lock_guard<std::mutex> lk(g_imu);
  std::string s(p, n);
  auto it = g_imap.find(s);
  if (it != g_imap.end()) return LONGBIT | it->second;
  u32 id = (u32)g_istr.size();
  g_istr.push_back(s);
  g_imap.emplace(s, id);
  return LONGBIT | id;
}

// p[0..15] must be readable.
__attribute__((always_inline)) static inline u64 pack7(const char* p, u32 n) {
  if (LIKELY(n - 1 < 9)) {
    u64 w;
    memcpy(&w, p, 8);
    u64 hi = 0, pad = 0;
    if (n < 8) {
      u64 keep = (~0ULL) >> (64 - 8 * n);
      w &= keep;
      pad = 0x0101010101010101ULL & ~keep;
    } else if (n == 9) {
      hi = (uint8_t)p[8];
      pad = 0;
    }
    u64 x = w | pad;
    u64 bad = ((w | hi) & 0x8080808080808080ULL) | ((x - 0x0101010101010101ULL) & ~x & 0x8080808080808080ULL);
    if (LIKELY(!bad && !(n == 9 && hi == 0))) {
      u64 be = __builtin_bswap64(w);
#if defined(__BMI2__)
      u64 pk = _pext_u64(be, 0x7F7F7F7F7F7F7F7FULL);
#else
      u64 pk = 0;
      for (int i = 7; i >= 0; i--) pk = (pk << 7) | ((be >> (8 * i)) & 0x7F);
#endif
      return (pk << 7) | hi;
    }
  }
  return intern_slow(p, n);
}

static u64 encode_str(const std::string& s) {
  if (s.size() <= 9) {
    char buf[32];
    memset(buf, 0, sizeof buf);
    memcpy(buf, s.data(), s.size());
    return pack7(buf, (u32)s.size());
  }
  return intern_slow(s.data(), (u32)s.size());
}

// no locking: only call when no thread can be interning
static inline u32 decode_fast(u64 code, char* out) {
  if (code & LONGBIT) {
    const std::string& s = g_istr[(u32)(code & 0xffffffffu)];
    memcpy(out, s.data(), s.size());
    return (u32)s.size();
  }
  u32 n = 0;
  for (int i = 0; i < 9; i++) {
    u32 c = (u32)((code >> (7 * (8 - i))) & 0x7F);
    if (!c) break;
    out[n++] = (char)c;
  }
  return n;
}
static void decode(u64 code, std::string& out) {
  if (code & LONGBIT) {
    std::lock_guard<std::mutex> lk(g_imu);
    out = g_istr[(u32)(code & 0xffffffffu)];
    return;
  }
  char buf[16];
  u32 n = decode_fast(code, buf);
  out.assign(buf, n);
}

__attribute__((noinline)) static bool key_less(u64 a, u64 b) {
  if (!((a | b) & LONGBIT)) return a < b;
  std::string x, y;
  decode(a, x);
  decode(b, y);
  return x < y;
}

// ---------------------------------------------------------------- timestamps
static inline i64 days_from_civil(i64 y, i64 m, i64 d) {
  y -= m <= 2;
  i64 era = (y >= 0 ? y : y - 399) / 400;
  i64 yoe = y - era * 400;
  i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}
static inline bool is_leap(i64 y) { return (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0); }
static inline u32 dim(i64 y, u32 m) {
  static const u32 D[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  return m == 2 ? (is_leap(y) ? 29 : 28) : D[m];
}
static const u32 TY0 = 1900, TYN = 400;
static i32 g_mstart[TYN * 12];
static uint8_t g_mdim[TYN * 12];
static void init_dates() {
  for (u32 y = 0; y < TYN; y++)
    for (u32 m = 1; m <= 12; m++) {
      g_mstart[y * 12 + m - 1] = (i32)days_from_civil(TY0 + y, m, 1);
      g_mdim[y * 12 + m - 1] = (uint8_t)dim(TY0 + y, m);
    }
}

// "YYYY-MM-DDTHH:MM:SS" followed by c-word check; returns ms without zone
template <u32 CMASK, u32 CVAL>
__attribute__((always_inline)) static inline bool civil_core(const char* p, i64& out) {
  u64 a, b;
  u32 c;
  memcpy(&a, p, 8);
  memcpy(&b, p + 8, 8);
  memcpy(&c, p + 16, 4);
  if ((a & 0xFF0000FF00000000ULL) != 0x2D00002D00000000ULL) return false;
  if ((b & 0x0000FF0000FF0000ULL) != 0x00003A0000540000ULL) return false;
  if ((c & CMASK) != CVAL) return false;
  const u64 Z = 0x3030303030303030ULL;
  u64 da = (a ^ Z) & 0x00FFFF00FFFFFFFFULL, db = (b ^ Z) & 0xFFFF00FFFF00FFFFULL;
  u32 dc = (c ^ 0x30303030u) & 0x00FFFF00u;
  if (((da | db) & 0xF0F0F0F0F0F0F0F0ULL) | (dc & 0xF0F0F0F0u)) return false;
  if ((((da + 0x0606060606060606ULL) | (db + 0x0606060606060606ULL)) & 0x1010101010101010ULL) |
      ((dc + 0x06060606u) & 0x10101010u))
    return false;
#define BY(x, i) ((u32)((x) >> (8 * (i))) & 0xFF)
  u32 Y = BY(da, 0) * 1000 + BY(da, 1) * 100 + BY(da, 2) * 10 + BY(da, 3);
  u32 M = BY(da, 5) * 10 + BY(da, 6);
  u32 D = BY(db, 0) * 10 + BY(db, 1);
  u32 h = BY(db, 3) * 10 + BY(db, 4);
  u32 mi = BY(db, 6) * 10 + BY(db, 7);
  u32 s = BY(dc, 1) * 10 + BY(dc, 2);
#undef BY
  if (M - 1 > 11 || h > 23 || mi > 59 || s > 59 || D == 0) return false;
  i64 days;
  if (LIKELY(Y - TY0 < TYN)) {
    u32 k = (Y - TY0) * 12 + M - 1;
    if (D > g_mdim[k]) return false;
    days = g_mstart[k] + (i64)D - 1;
  } else {
    if (D > dim(Y, M)) return false;
    days = days_from_civil(Y, M, D);
  }
  out = (days * 86400 + (i64)(h * 3600 + mi * 60 + s)) * 1000;
  return true;
}

// Date-hour cache: "YYYY-MM-DDTHH" (13 bytes, validated on insert) -> ms of that hour.
// Data spans a few hundred distinct hours, so most timestamps only decode ":MM:SS".
struct HourEnt {
  u64 a, b5;
  i64 ms;
};
static thread_local HourEnt g_hc[1024];
template <bool ZULU>
__attribute__((always_inline)) static inline bool civil_cached(const char* p, i64& out) {
#ifdef HOURCACHE
  u64 a, b, t;
  memcpy(&a, p, 8);
  memcpy(&b, p + 8, 8);
  memcpy(&t, p + 12, 8);
  u64 b5 = b & 0xFFFFFFFFFFULL;
  u64 h = ((a * 0x9E3779B97F4A7C15ULL) ^ (b5 * 0x165667B19E3779F9ULL)) >> 54;
  HourEnt& e = g_hc[h];
  if (LIKELY(e.a == a && e.b5 == b5)) {
    // t bytes: 0 'H', 1 ':', 2-3 MM, 4 ':', 5-6 SS, 7 'Z' or sign
    if ((t & 0x000000FF0000FF00ULL) != 0x0000003A00003A00ULL) return false;
    if (ZULU && (t >> 56) != 'Z') return false;
    u64 x = (t ^ 0x3030303030303030ULL) & 0x00FFFF00FFFF0000ULL;
    if ((x & 0xF0F0F0F0F0F0F0F0ULL) | ((x + 0x0606060606060606ULL) & 0x1010101010101010ULL)) return false;
    u32 mm = (u32)((x >> 16) & 0xFF) * 10 + (u32)((x >> 24) & 0xFF);
    u32 ss = (u32)((x >> 40) & 0xFF) * 10 + (u32)((x >> 48) & 0xFF);
    if (mm > 59 || ss > 59) return false;
    out = e.ms + (i64)(mm * 60 + ss) * 1000;
    return true;
  }
  bool ok = ZULU ? civil_core<0xFF0000FFu, 0x5A00003Au>(p, out) : civil_core<0x000000FFu, 0x0000003Au>(p, out);
  if (ok) {
    u32 mm = (u32)(p[14] - '0') * 10 + (u32)(p[15] - '0');
    u32 ss = (u32)(p[17] - '0') * 10 + (u32)(p[18] - '0');
    e.a = a;
    e.b5 = b5;
    e.ms = out - (i64)(mm * 60 + ss) * 1000;
  }
  return ok;
#else
  return ZULU ? civil_core<0xFF0000FFu, 0x5A00003Au>(p, out) : civil_core<0x000000FFu, 0x0000003Au>(p, out);
#endif
}

// p[0..n+15] readable
__attribute__((always_inline)) static inline bool parse_ts(const char* p, u32 n, i64& out) {
  if (n == 20) {
    if (p[4] == '-') return civil_cached<true>(p, out);
  } else if (n == 25) {
    if (p[4] != '-') goto digits;
    i64 base;
    if (!civil_cached<false>(p, base)) return false;
    char sg = p[19];
    u32 oh0 = (u32)(uint8_t)p[20] - '0', oh1 = (u32)(uint8_t)p[21] - '0', om0 = (u32)(uint8_t)p[23] - '0',
        om1 = (u32)(uint8_t)p[24] - '0';
    if ((sg != '+' && sg != '-') || p[22] != ':' || oh0 > 9 || oh1 > 9 || om0 > 9 || om1 > 9) return false;
    u32 oh = oh0 * 10 + oh1, om = om0 * 10 + om1;
    if (oh > 23 || om > 59) return false;
    i64 off = (i64)(oh * 60 + om) * 60000;
    out = sg == '+' ? base - off : base + off;
    return true;
  }
digits:
  if (LIKELY(n - 9 < 8)) {  // 9..16 chars: SWAR
    u32 k = n - 8;           // 1..8 leading chars
    u64 w, lo;
    memcpy(&w, p, 8);
    memcpy(&lo, p + k, 8);
    w <<= 8 * (8 - k);
    w |= 0x3030303030303030ULL & ((~0ULL) >> (8 * k));
    const u64 Z = 0x3030303030303030ULL;
    u64 xa = w ^ Z, xb = lo ^ Z;
    if ((((xa | xb) & 0xF0F0F0F0F0F0F0F0ULL) | (((xa + 0x0606060606060606ULL) | (xb + 0x0606060606060606ULL)) &
                                                 0x1010101010101010ULL)) == 0) {
      auto conv = [](u64 v) -> u64 {
        v = (v * 10 + (v >> 8)) & 0x00FF00FF00FF00FFULL;
        v = (v * 100 + (v >> 16)) & 0x0000FFFF0000FFFFULL;
        v = (v * 10000 + (v >> 32)) & 0x00000000FFFFFFFFULL;
        return v;
      };
      out = (i64)(conv(xa) * 100000000ULL + conv(xb));
      return true;
    }
    return false;
  }
  if (n == 0) return false;
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  u32 sig = n - i;
  if (sig > 17) {
    return false;  // > 1e17 ms: far outside any window (or not digits: bad anyway)
  }
  i64 v = 0;
  for (u32 j = i; j < n; j++) {
    u32 d = (u32)(uint8_t)p[j] - '0';
    if (d > 9) return false;
    v = v * 10 + d;
  }
  out = v;
  return true;
}

static inline bool parse_price(const char* p, u32 n, u32& out) {
  if (n == 0) return false;
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  if (n - i > 8) return false;
  u32 v = 0;
  for (u32 j = i; j < n; j++) {
    u32 d = (u32)(uint8_t)p[j] - '0';
    if (d > 9) return false;
    v = v * 10 + d;
  }
  if (v < 1 || v > 10000000) return false;
  out = v;
  return true;
}

// ---------------------------------------------------------------- CSV scanning
struct Fld {
  const char* p;
  u32 n;
};

static inline void masks64(const char* p, u64& c, u64& q, u64& nl) {
#if defined(__AVX2__)
  __m256i a = _mm256_loadu_si256((const __m256i*)p);
  __m256i b = _mm256_loadu_si256((const __m256i*)(p + 32));
  __m256i C = _mm256_set1_epi8(','), Q = _mm256_set1_epi8('"'), N = _mm256_set1_epi8('\n');
  c = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, C)) |
      ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, C)) << 32);
  q = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, Q)) |
      ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, Q)) << 32);
  nl = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, N)) |
       ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, N)) << 32);
#elif defined(__ARM_NEON)
  const uint8x16_t bits = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
  uint8x16_t v0 = vld1q_u8((const uint8_t*)p), v1 = vld1q_u8((const uint8_t*)p + 16),
             v2 = vld1q_u8((const uint8_t*)p + 32), v3 = vld1q_u8((const uint8_t*)p + 48);
  auto mm = [&](uint8_t ch) -> u64 {
    uint8x16_t k = vdupq_n_u8(ch);
    uint8x16_t t0 = vandq_u8(vceqq_u8(v0, k), bits), t1 = vandq_u8(vceqq_u8(v1, k), bits),
               t2 = vandq_u8(vceqq_u8(v2, k), bits), t3 = vandq_u8(vceqq_u8(v3, k), bits);
    uint8x16_t s0 = vpaddq_u8(t0, t1), s1 = vpaddq_u8(t2, t3);
    s0 = vpaddq_u8(s0, s1);
    s0 = vpaddq_u8(s0, s0);
    return vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0);
  };
  c = mm(',');
  q = mm('"');
  nl = mm('\n');
#else
  c = q = nl = 0;
  for (int i = 0; i < 64; i++) {
    c |= (u64)(p[i] == ',') << i;
    q |= (u64)(p[i] == '"') << i;
    nl |= (u64)(p[i] == '\n') << i;
  }
#endif
}

// Generic scalar tokenizer for one line [p, e).
template <int N>
static bool tok_scalar(const char* p, const char* e, Fld* f);

static inline u32 ctz2(u64 lo, u64 hi) { return lo ? (u32)__builtin_ctzll(lo) : 64 + (u32)__builtin_ctzll(hi); }

// Scan lines starting at p (a line start) while the line start is < cend.
// fend is the end of valid data; >= 192 readable bytes follow it. If eof, a final line
// without '\n' is complete; otherwise it is left unprocessed. Returns the start of the
// first line not processed.
// Masks are computed once per 64-byte block into a 3-block window; each line is then cut
// with a fixed number of bit extractions.
template <int N, class F>
static const char* scan_old(const char* p, const char* cend, const char* fend, bool eof, F&& fn) {
  Fld f[N];
  const char* base = p;
  u64 C0, Q0, N0, C1, Q1, N1, C2, Q2, N2;
  auto blockmask = [&](const char* b, u64& c, u64& q, u64& nl) {
    masks64(b, c, q, nl);
    if (UNLIKELY(b + 64 > fend)) {
      u64 vm = b >= fend ? 0 : ((1ULL << (fend - b)) - 1);
      c &= vm;
      q &= vm;
      nl &= vm;
    }
  };
  blockmask(base, C0, Q0, N0);
  blockmask(base + 64, C1, Q1, N1);
  blockmask(base + 128, C2, Q2, N2);
  const char* ls = p;
  while (ls < fend) {
    if (ls >= cend) return ls;
    size_t o = (size_t)(ls - base);
    while (o >= 64) {
      base += 64;
      o -= 64;
      C0 = C1; Q0 = Q1; N0 = N1;
      C1 = C2; Q1 = Q2; N1 = N2;
      blockmask(base + 128, C2, Q2, N2);
    }
    const u32 sh = (u32)o, rs = 63 - sh;
    u64 nlo = (N0 >> sh) | ((N1 << 1) << rs), nhi = (N1 >> sh) | ((N2 << 1) << rs);
    u32 L;
    if (LIKELY(nlo | nhi)) {
      L = ctz2(nlo, nhi);
    } else {
      size_t avail = (size_t)(fend - ls);
      if (avail >= 128) {
        // a line of 128+ bytes: slow path
        const char* e = (const char*)memchr(ls + 128, '\n', avail - 128);
        if (!e) {
          if (!eof) return ls;
          e = fend;
        }
        if (tok_scalar<N>(ls, e, f)) fn(f);
        ls = e + 1;
        base = ls;
        if (ls < fend) {
          blockmask(base, C0, Q0, N0);
          blockmask(base + 64, C1, Q1, N1);
          blockmask(base + 128, C2, Q2, N2);
        }
        continue;
      }
      if (!eof) return ls;
      L = (u32)avail;  // final line without newline
    }
    u64 clo = (C0 >> sh) | ((C1 << 1) << rs), chi = (C1 >> sh) | ((C2 << 1) << rs);
    u64 qlo = (Q0 >> sh) | ((Q1 << 1) << rs), qhi = (Q1 >> sh) | ((Q2 << 1) << rs);
    // keep bits < L
    u64 mlo = L >= 64 ? ~0ULL : ((1ULL << L) - 1);
    u64 mhi = L <= 64 ? 0 : (L >= 128 ? ~0ULL : ((1ULL << (L - 64)) - 1));
    clo &= mlo;
    chi &= mhi;
    qlo &= mlo;
    qhi &= mhi;
    const char* line = ls;
    ls += L + 1;
    if (UNLIKELY((u32)(__builtin_popcountll(clo) + __builtin_popcountll(chi)) != (u32)(N - 1))) continue;
    const u64 clo0 = clo, chi0 = chi;
    (void)clo0;
    (void)chi0;
    u32 st = 0;
    for (int i = 0; i < N - 1; i++) {
      u32 e = ctz2(clo, chi);
      if (clo) clo &= clo - 1; else chi &= chi - 1;
      f[i].p = line + st;
      f[i].n = e - st;
      st = e + 1;
    }
    f[N - 1].p = line + st;
    f[N - 1].n = L - st;
#ifndef NO_QMASK
    if (qlo | qhi) {
      typedef unsigned __int128 u128;
      u128 Cm = (u128)clo0 | ((u128)chi0 << 64);
      u128 Qm = (u128)qlo | ((u128)qhi << 64);
      u128 one = 1;
      u128 lm = (one << L) - 1;
      u128 SEP = Cm | (one << L);
      u128 FS = ((Cm << 1) | one) & lm;
      u128 FE = SEP >> 1;
      u128 QS = Qm & FS, QE = Qm & FE;
      u128 M = ~SEP & lm;
      if (UNLIKELY(Qm != (QS | QE) || QE != (((M + QS) & SEP) >> 1) || (QS & QE))) continue;
      while (QS) {
        u64 lo = (u64)QS, hi = (u64)(QS >> 64);
        u32 sp = ctz2(lo, hi);
        u128 below = (one << sp) - 1;
        u128 cb = Cm & below;
        u32 idx = (u32)(__builtin_popcountll((u64)cb) + __builtin_popcountll((u64)(cb >> 64)));
        f[idx].p++;
        f[idx].n -= 2;
        QS &= QS - 1;
      }
    }
#else
    if (qlo | qhi) {
      u32 qc = (u32)(__builtin_popcountll(qlo) + __builtin_popcountll(qhi));
      u32 pairs = 0;
      for (int i = 0; i < N; i++) {
        const char* fp = f[i].p;
        u32 n = f[i].n;
        u32 last = n ? n - 1 : 0;
        u32 isq = (u32)(n >= 2) & (u32)(fp[0] == '"') & (u32)(fp[last] == '"');
        f[i].p = fp + isq;
        f[i].n = n - 2 * isq;
        pairs += isq;
      }
      if (pairs * 2 != qc) continue;
    }
#endif
    fn(f);
  }
  return ls < fend ? ls : fend;
}

static inline u32 tz64(u64 x) { return x ? (u32)__builtin_ctzll(x) : 64; }
typedef unsigned __int128 u128;
static inline u32 tz128(u128 x) {
  u64 lo = (u64)x, hi = (u64)(x >> 64);
  u32 tl = tz64(lo), th = tz64(hi);
  return tl + (lo == 0 ? th : 0);
}
static inline u32 pop128(u128 x) { return (u32)(__builtin_popcountll((u64)x) + __builtin_popcountll((u64)(x >> 64))); }

// Two-stage scanner: stage 1 computes comma/quote/newline masks for every 64-byte block
// of [p, fend); stage 2 cuts lines branch-free from a 3-block window of those masks.
// Same contract as scan_old.
template <int N, class F>
static const char* scan(const char* p, const char* cend, const char* fend, bool eof, F&& fn) {
#ifndef NEW_SCAN
  return scan_old<N>(p, cend, fend, eof, fn);
#endif
  static thread_local std::vector<u64> MV;
  const size_t len = (size_t)(fend - p);
  const size_t nb = (len + 63) / 64;
  if (MV.size() < (nb + 4) * 3) MV.resize((nb + 4) * 3 + 1024);
  u64* M = MV.data();  // triples: C, Q, N per block
  for (size_t b = 0; b < nb; b++) {
    u64 c, q, nl;
    masks64(p + b * 64, c, q, nl);
    M[3 * b] = c;
    M[3 * b + 1] = q;
    M[3 * b + 2] = nl;
  }
  if (len & 63) {
    u64 vm = (1ULL << (len & 63)) - 1;
    M[3 * (nb - 1)] &= vm;
    M[3 * (nb - 1) + 1] &= vm;
    M[3 * (nb - 1) + 2] &= vm;
  }
  for (size_t b = nb; b < nb + 3; b++) M[3 * b] = M[3 * b + 1] = M[3 * b + 2] = 0;
  Fld f[N];
  size_t off = 0;
  while (off < len) {
    if (p + off >= cend) return p + off;
    const size_t blk = off >> 6;
    const u32 sh = (u32)(off & 63), rs = 63 - sh;
    const u64* w = M + 3 * blk;
    u64 nlo = (w[2] >> sh) | ((w[5] << 1) << rs), nhi = (w[5] >> sh) | ((w[8] << 1) << rs);
    const char* line = p + off;
    u32 L;
    if (LIKELY(nlo | nhi)) {
      L = tz128((u128)nlo | ((u128)nhi << 64));
    } else {
      size_t avail = len - off;
      if (avail >= 128) {
        const char* e = (const char*)memchr(line + 128, '\n', avail - 128);
        if (!e) {
          if (!eof) return line;
          e = fend;
        }
        if (tok_scalar<N>(line, e, f)) fn(f);
        off = (size_t)(e - p) + 1;
        continue;
      }
      if (!eof) return line;
      L = (u32)avail;
    }
    off += L + 1;
    u128 lm = (((u128)1) << L) - 1;
    u128 Cm = ((u128)((w[0] >> sh) | ((w[3] << 1) << rs)) | ((u128)((w[3] >> sh) | ((w[6] << 1) << rs)) << 64)) & lm;
    if (UNLIKELY(pop128(Cm) != (u32)(N - 1))) continue;
    u128 Qm = ((u128)((w[1] >> sh) | ((w[4] << 1) << rs)) | ((u128)((w[4] >> sh) | ((w[7] << 1) << rs)) << 64)) & lm;
    u128 Cx = Cm;
    u32 st = 0;
    for (int i = 0; i < N - 1; i++) {
      u32 e = tz128(Cx);
      Cx &= Cx - 1;
      f[i].p = line + st;
      f[i].n = e - st;
      st = e + 1;
    }
    f[N - 1].p = line + st;
    f[N - 1].n = L - st;
    if (Qm) {
      u128 one = 1;
      u128 SEP = Cm | (one << L);
      u128 FS = ((Cm << 1) | one) & lm;
      u128 FE = SEP >> 1;
      u128 QS = Qm & FS, QE = Qm & FE;
      u128 Mm = ~SEP & lm;
      if (UNLIKELY(Qm != (QS | QE) || QE != (((Mm + QS) & SEP) >> 1) || (QS & QE))) continue;
      for (int i = 0; i < N; i++) {
        u32 s0 = (u32)(f[i].p - line);
        u32 has = (u32)(QS >> s0) & 1;
        f[i].p += has;
        f[i].n -= 2 * has;
      }
    }
    fn(f);
  }
  return fend;
}

// Generic scalar tokenizer for one line [p, e) (small files).
template <int N>
static bool tok_scalar(const char* p, const char* e, Fld* f) {
  int nf = 0;
  const char* s = p;
  while (true) {
    if (nf == N) return false;
    if (s < e && *s == '"') {
      const char* q = (const char*)memchr(s + 1, '"', (size_t)(e - (s + 1)));
      if (!q) return false;
      f[nf].p = s + 1;
      f[nf].n = (u32)(q - (s + 1));
      nf++;
      s = q + 1;
      if (s == e) break;
      if (*s != ',') return false;
      s++;
    } else {
      const char* q = s;
      while (q < e && *q != ',' && *q != '"') q++;
      if (q < e && *q == '"') return false;
      f[nf].p = s;
      f[nf].n = (u32)(q - s);
      nf++;
      if (q == e) break;
      s = q + 1;
    }
  }
  return nf == N;
}

// ---------------------------------------------------------------- hash tables
static inline u64 hmix(u64 x, u64 k) { return (x ^ (x >> 29)) * k; }
static const u64 K1 = 0x9E3779B97F4A7C15ULL, K2 = 0xC2B2AE3D27D4EB4FULL, K3 = 0x165667B19E3779F9ULL,
                 K4 = 0xD6E8FEB86659FD93ULL, K5 = 0xFF51AFD7ED558CCDULL;

struct SmallTab {  // u64 (nonzero) -> u32
  std::vector<u64> keys;
  std::vector<u32> vals;
  int shift = 64;
  u64 mask = 0;
  void init(size_t n) {
    size_t cap = 16;
    int lg = 4;
    while (cap < n * 4) { cap <<= 1; lg++; }
    keys.assign(cap, 0);
    vals.assign(cap, 0);
    mask = cap - 1;
    shift = 64 - lg;
  }
  void put(u64 k, u32 v) {
    u64 i = hmix(k, K5) >> shift;
    while (keys[i] && keys[i] != k) i = (i + 1) & mask;
    keys[i] = k;
    vals[i] = v;
  }
  inline bool get(u64 k, u32& v) const {
    u64 i = hmix(k, K5) >> shift;
    while (true) {
      u64 x = keys[i];
      if (x == k) { v = vals[i]; return true; }
      if (!x) return false;
      i = (i + 1) & mask;
    }
  }
};

struct LSlot {
  std::atomic<u64> key;
  u64 name;
};
struct LinkTab {
  LSlot* s = nullptr;
  u32* parent = nullptr;
  u64 mask = 0;
  int shift = 64;
  void init(size_t n) {
    size_t cap = 1024;
    int lg = 10;
    while (cap < n * 2) { cap <<= 1; lg++; }
    s = (LSlot*)big_alloc(cap * sizeof(LSlot));
    parent = (u32*)big_alloc(cap * sizeof(u32));
    mask = cap - 1;
    shift = 64 - lg;
  }
  inline u64 slot_of(u64 k) const { return hmix(k, K4) >> shift; }
  inline u32 insert(u64 k, std::vector<u32>& fresh) {
    u64 i = slot_of(k);
    while (true) {
      u64 x = s[i].key.load(std::memory_order_relaxed);
      if (x == k) return (u32)i;
      if (x == 0) {
        u64 exp = 0;
        if (s[i].key.compare_exchange_strong(exp, k, std::memory_order_relaxed)) {
          s[i].name = k;
          parent[i] = (u32)i;
          fresh.push_back((u32)i);
          return (u32)i;
        }
        if (exp == k) return (u32)i;
      }
      i = (i + 1) & mask;
    }
  }
  inline void prefetch(u64 k) const { __builtin_prefetch((const void*)&s[slot_of(k)]); }
  inline u64 lookup(u64 k) const {
    u64 i = slot_of(k);
    while (true) {
      u64 x = s[i].key.load(std::memory_order_relaxed);
      if (x == k) return s[i].name;
      if (!x) return k;
      i = (i + 1) & mask;
    }
  }
  u32 find(u32 x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  }
};

// ---------------------------------------------------------------- records
struct EKey {  // event copy, partitioned by event_id
  u64 eid;
  i32 ing;  // ingest seconds since window_start (clamped)
  u32 ref;  // payload index or NOREF if this copy is invalid
};
struct EPay {  // valid copy payload
  u64 user;
  u32 ts;  // ms since window_start
  u32 pc;  // price | cat << 24 (purchase), 0 otherwise
};
struct Rec2 {  // surviving event, partitioned by person
  u64 person;
  u32 ts;
  u32 pc;
};
struct ClickRec {
  u64 who;  // user, then person
  u64 cid;
  u32 ts;  // ms since window_start - 7d
  u32 camp;
};

// Partitioned buffer: a region per (thread, partition) in one big array, plus overflow
// chunks from the same array. Every element has a stable global index (its "ref").
template <class T>
struct PartBuf {
  static const u32 CH = 512;
  T* base = nullptr;
  size_t cap = 0, main_n = 0, total_n = 0;
  int P = 0, NTh = 0;
  std::atomic<size_t> ovf_next{0};
  std::vector<T*> cur, end;
  std::vector<std::vector<size_t>> extra;
  static size_t total_for(size_t cap_per, int nth, int parts) {
    size_t m = cap_per * nth * parts;
    return m + m / 2 + (size_t)nth * parts * CH * 2 + CH;
  }
  void init(size_t cap_per, int nth, int parts, void* mem = nullptr, size_t mem_n = 0) {
    cap = cap_per;
    P = parts;
    NTh = nth;
    main_n = cap * nth * parts;
    total_n = mem ? mem_n : total_for(cap_per, nth, parts);
    base = mem ? (T*)mem : (T*)big_alloc(total_n * sizeof(T));
    ovf_next.store(main_n);
    size_t R = (size_t)nth * parts;
    cur.resize(R);
    end.resize(R);
    extra.assign(R, {});
    for (size_t i = 0; i < R; i++) {
      cur[i] = base + i * cap;
      end[i] = cur[i] + cap;
    }
  }
  __attribute__((noinline)) T* grow(size_t i) {
    size_t s = ovf_next.fetch_add(CH);
    if (s + CH > total_n) {
      fprintf(stderr, "partition buffer overflow\n");
      exit(1);
    }
    extra[i].push_back(s);
    end[i] = base + s + CH;
    return base + s;
  }
  inline T* slot(int t, u32 p) {
    size_t i = (size_t)t * P + p;
    T* w = cur[i];
    if (UNLIKELY(w == end[i])) w = grow(i);
    cur[i] = w + 1;
    return w;
  }
  inline void push(int t, u32 p, const T& x) { *slot(t, p) = x; }
  inline u32 ref(const T* x) const { return (u32)(x - base); }
  size_t count(int t, u32 p) const {
    size_t i = (size_t)t * P + p;
    const auto& ex = extra[i];
    if (ex.empty()) return (size_t)(cur[i] - (base + i * cap));
    return cap + (ex.size() - 1) * CH + (size_t)(cur[i] - (base + ex.back()));
  }
  // fn(const T* begin, const T* end) for each contiguous run of (t, p)
  template <class F>
  void runs(int t, u32 p, F&& fn) const {
    size_t i = (size_t)t * P + p;
    const auto& ex = extra[i];
    const T* r0 = base + i * cap;
    if (ex.empty()) {
      fn(r0, (const T*)cur[i]);
      return;
    }
    fn(r0, r0 + cap);
    for (size_t k = 0; k < ex.size(); k++) {
      const T* c0 = base + ex[k];
      fn(c0, k + 1 == ex.size() ? (const T*)cur[i] : c0 + CH);
    }
  }
  template <class F>
  void each(int t, u32 p, F&& fn) const {
    runs(t, p, [&](const T* a, const T* b) {
      for (; a != b; a++) fn(*a);
    });
  }
};

// Write-combining partitioned buffer for 16-byte records: each (thread, partition) stages a
// cache line of 4 records and writes it out with streaming stores (no read-for-ownership).
static inline void stream64(void* dst, const void* src) {
#if defined(__AVX2__)
  __m256i a = _mm256_load_si256((const __m256i*)src), b = _mm256_load_si256((const __m256i*)src + 1);
  _mm256_stream_si256((__m256i*)dst, a);
  _mm256_stream_si256((__m256i*)dst + 1, b);
#else
  memcpy(dst, src, 64);
#endif
}
template <class T>
struct WCBuf : PartBuf<T> {
  static_assert(sizeof(T) == 16, "WCBuf needs 16-byte records");
  struct alignas(64) Line {
    T v[4];
  };
  Line* stage = nullptr;
  uint8_t* scnt = nullptr;
  void init(size_t cap_per, int nth, int parts, void* mem = nullptr, size_t mem_n = 0) {
    PartBuf<T>::init((cap_per + 3) & ~(size_t)3, nth, parts, mem, mem_n);
    size_t R = (size_t)nth * parts;
    stage = (Line*)big_alloc(R * sizeof(Line));
    scnt = (uint8_t*)big_alloc(R);
  }
  inline u32 put(int t, u32 p, const T& x) {
    size_t i = (size_t)t * this->P + p;
    u32 k = scnt[i];
    T* c = this->cur[i];
    if (UNLIKELY(k == 0 && c == this->end[i])) {
      c = this->grow(i);
      this->cur[i] = c;
    }
    u32 ref = (u32)(c - this->base) + k;
    stage[i].v[k] = x;
    if (++k == 4) {
      stream64(c, &stage[i]);
      this->cur[i] = c + 4;
      k = 0;
    }
    scnt[i] = (uint8_t)k;
    return ref;
  }
  void finish(int t) {
    for (int p = 0; p < this->P; p++) {
      size_t i = (size_t)t * this->P + p;
      u32 k = scnt[i];
      if (k) {
        memcpy(this->cur[i], stage[i].v, k * sizeof(T));
        this->cur[i] += k;
        scnt[i] = 0;
      }
    }
#if defined(__AVX2__)
    _mm_sfence();
#endif
  }
};

// ---------------------------------------------------------------- globals
static int NT = 8;
static i64 WS, WE, CB;
static i64 DAY0;
static int NDAYS;
static int PBITS, P;      // user partitions (payload, raw clicks)
static int P1BITS, P1;    // event_id partitions (dedup keys)
static int P2BITS, P2;    // person partitions (sessions)
static SmallTab g_prod;
static SmallTab g_camp;
static std::vector<std::string> g_cats;
static u32 g_unknown_cat;
static std::vector<std::string> g_camp_id, g_camp_ch;
static LinkTab g_links;
static thread_local u64 g_exp_sink = 0;

struct FileMap {
  int fd;
  const char* map;
  size_t size;
  int kind;  // 0 events, 1 clicks, 2 links
};
struct Chunk {
  int file;
  size_t beg, end;
};
static std::vector<FileMap> g_files;

struct CD {
  i64 sessions, purch, rev, assisted;
};
struct UserRow {
  u64 name;
  i32 day;
  u32 topcat;
  i64 sessions, events, rev;
};

#ifdef USE_WC
static WCBuf<EKey> g_keys;   // by event_id
static WCBuf<EPay> g_pay;    // by user (same hash bits as the link table)
static WCBuf<Rec2> g_rec2;   // by person (reuses g_keys memory)
#else
template <class T>
struct PlainBuf : PartBuf<T> {
  inline u32 put(int t, u32 p, const T& x) {
    T* w = this->slot(t, p);
    *w = x;
    return this->ref(w);
  }
  void finish(int) {}
};
static PlainBuf<EKey> g_keys;
static PlainBuf<EPay> g_pay;
static PlainBuf<Rec2> g_rec2;
#endif
static PartBuf<ClickRec> g_craw;  // valid clicks by user
static PartBuf<ClickRec> g_cper;  // valid clicks by person
static std::atomic<u64>* g_loser = nullptr;

struct Pend {
  u64 user, eid;
  i32 ing;
  u32 ts, pc;
};
struct alignas(64) TL {
  Pend ring[16];
  u32 rh = 0, rn = 0;
  std::vector<std::pair<u32, u32>> links;
  std::vector<u32> fresh;
  std::vector<UserRow> rows;
  std::vector<CD> cd;
  // day-major per-thread accumulators: index day * ncamp + camp
  std::vector<u32> a_sess, a_purch, a_assist;
  std::vector<i64> a_rev;
};
static std::vector<TL> g_tl;

static std::string read_file(const std::string& path) {
  std::string s;
  FILE* f = fopen(path.c_str(), "rb");
  if (!f) return s;
  char buf[1 << 16];
  size_t n;
  while ((n = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, n);
  fclose(f);
  return s;
}

static std::vector<std::string> list_csv(const std::string& dir) {
  std::vector<std::string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (struct dirent* e = readdir(d)) {
    std::string n = e->d_name;
    if (n.size() > 4 && n.compare(n.size() - 4, 4, ".csv") == 0) out.push_back(dir + "/" + n);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}

static void open_file(const std::string& path, int kind) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) return;
  struct stat st;
  fstat(fd, &st);
  size_t n = (size_t)st.st_size;
  if (n == 0) {
    close(fd);
    return;
  }
  const char* mp = nullptr;
#ifdef USE_MMAP
  size_t pg = (size_t)sysconf(_SC_PAGESIZE);
  size_t total = (n + pg - 1) / pg * pg + (64 << 10);
  void* res = mmap(nullptr, total, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  void* m = mmap(res, n, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0);
  if (res == MAP_FAILED || m == MAP_FAILED) {
    fprintf(stderr, "mmap failed\n");
    exit(1);
  }
  mp = (const char*)m;
#endif
  g_files.push_back({fd, mp, n, kind});
}

template <int N, class F>
static void each_line(const std::string& s, F fn) {
  const char* p = s.data();
  const char* e = p + s.size();
  const char* nl = (const char*)memchr(p, '\n', (size_t)(e - p));
  if (!nl) return;
  p = nl + 1;
  while (p < e) {
    const char* q = (const char*)memchr(p, '\n', (size_t)(e - p));
    const char* le = q ? q : e;
    Fld f[N];
    if (tok_scalar<N>(p, le, f)) fn(f);
    p = q ? q + 1 : e;
  }
}

static bool find_json_str(const std::string& s, const char* key, std::string& out) {
  std::string k = std::string("\"") + key + "\"";
  size_t i = s.find(k);
  if (i == std::string::npos) return false;
  i = s.find(':', i + k.size());
  if (i == std::string::npos) return false;
  i = s.find('"', i);
  if (i == std::string::npos) return false;
  size_t j = s.find('"', i + 1);
  if (j == std::string::npos) return false;
  out = s.substr(i + 1, j - i - 1);
  return true;
}

// ---------------------------------------------------------------- phase 1: parse
static inline u32 p1_of(u64 eid) { return (u32)(hmix(eid, K1) >> (64 - P1BITS)); }
static inline u32 p2_of(u64 person) { return (u32)(hmix(person, K2) >> (64 - P2BITS)); }

static inline u32 pu_of(u64 user) { return (u32)(hmix(user, K4) >> (64 - PBITS)); }

#ifdef PARSE_LOOKUP
static inline void resolve_pend(int tid, const Pend& x) {
  u64 person = g_links.lookup(x.user);
  u32 ref = g_rec2.put(tid, p2_of(person), Rec2{person, x.ts, x.pc});
  g_keys.put(tid, p1_of(x.eid), EKey{x.eid, x.ing, ref});
}
static inline void push_pend(int tid, TL& tl, const Pend& x) {
  g_links.prefetch(x.user);
  if (tl.rn == 16) {
    resolve_pend(tid, tl.ring[tl.rh]);
    tl.ring[tl.rh] = x;
    tl.rh = (tl.rh + 1) & 15;
  } else {
    tl.ring[(tl.rh + tl.rn++) & 15] = x;
  }
}
static void flush_pend(int tid, TL& tl) {
  for (u32 i = 0; i < tl.rn; i++) resolve_pend(tid, tl.ring[(tl.rh + i) & 15]);
  tl.rn = 0;
  tl.rh = 0;
}
#endif

static const char* parse_events(int tid, const char* p, const char* cend, const char* fend, bool eof) {
  const i64 wlen = WE - WS;
#ifdef PARSE_LOOKUP
  TL& tl = g_tl[tid];
#endif
  return scan<7>(p, cend, fend, eof, [&](Fld* f) {
#ifdef EXP_SCANONLY
    g_tl[tid].rows.reserve(0);
    g_exp_sink += f[0].n + f[6].n;
    return;
#endif
    if (UNLIKELY(f[0].n == 0)) return;
    EKey k;
    k.eid = pack7(f[0].p, f[0].n);
    k.ref = NOREF;
    i64 ing;
    if (UNLIKELY(!parse_ts(f[3].p, f[3].n, ing))) ing = (i64)1 << 60;
    i64 ings = (ing - WS) / 1000;
    if (UNLIKELY(ings > INT32_MAX)) ings = INT32_MAX;
    if (UNLIKELY(ings < INT32_MIN)) ings = INT32_MIN;
    k.ing = (i32)ings;
    do {
      if (UNLIKELY(f[1].n == 0 || f[5].n == 0)) break;
      i64 ts;
      if (UNLIKELY(!parse_ts(f[2].p, f[2].n, ts))) break;
      ts -= WS;
      if (UNLIKELY((u64)ts >= (u64)wlen)) break;
      if (UNLIKELY(ing - (ts + WS) > DAYMS)) break;
      u32 pc = 0;
      const char* t = f[4].p;
      u32 w4;
      u64 w8;
      memcpy(&w4, t, 4);
      memcpy(&w8, t, 8);
      u32 isv = (u32)(f[4].n == 4) & ((u32)(w4 == 0x77656976u /*view*/) | (u32)(w4 == 0x74726163u /*cart*/));
      u32 isp = (u32)(f[4].n == 8) & (u32)(w8 == 0x6573616863727570ULL /*purchase*/);
      if (UNLIKELY(!(isv | isp))) break;
      if (UNLIKELY(isp)) {
        u32 price;
        if (!parse_price(f[6].p, f[6].n, price)) break;
        u32 cat;
        if (!g_prod.get(pack7(f[5].p, f[5].n), cat)) cat = g_unknown_cat;
        pc = price | (cat << 24);
      }
      u64 user = pack7(f[1].p, f[1].n);
#ifdef PARSE_LOOKUP
      push_pend(tid, tl, Pend{user, k.eid, k.ing, (u32)ts, pc});
      return;
#endif
#ifdef EXP_NOSCATTER
      k.ref = (u32)(user ^ ts ^ pc);
#else
      k.ref = g_pay.put(tid, pu_of(user), EPay{user, (u32)ts, pc});
#endif
    } while (0);
#ifdef EXP_NOSCATTER
    g_exp_sink += k.eid + k.ing + k.ref;
    return;
#endif
    g_keys.put(tid, p1_of(k.eid), k);
  });
}

static const char* parse_clicks(int tid, const char* p, const char* cend, const char* fend, bool eof) {
  const i64 clen = WE - CB;
  return scan<4>(p, cend, fend, eof, [&](Fld* f) {
    if (f[0].n == 0 || f[1].n == 0 || f[2].n == 0) return;
    i64 ts;
    if (!parse_ts(f[3].p, f[3].n, ts)) return;
    ts -= CB;
    if ((u64)ts >= (u64)clen) return;
    u32 camp;
    if (!g_camp.get(pack7(f[2].p, f[2].n), camp)) return;
    ClickRec c;
    c.who = pack7(f[1].p, f[1].n);
    c.cid = pack7(f[0].p, f[0].n);
    c.ts = (u32)ts;
    c.camp = camp;
    g_craw.push(tid, pu_of(c.who), c);
  });
}

static const char* parse_links(int tid, const char* p, const char* cend, const char* fend, bool eof) {
  TL& tl = g_tl[tid];
  return scan<2>(p, cend, fend, eof, [&](Fld* f) {
    if (f[0].n == 0 || f[1].n == 0) return;
    u32 a = g_links.insert(pack7(f[0].p, f[0].n), tl.fresh);
    u32 b = g_links.insert(pack7(f[1].p, f[1].n), tl.fresh);
    if (a != b) tl.links.push_back({a, b});
  });
}

static void run_union_find() {
  LinkTab& L = g_links;
  for (auto& tl : g_tl) {
    for (auto& pr : tl.links) {
      u32 a = L.find(pr.first), b = L.find(pr.second);
      if (a == b) continue;
      if (key_less(L.s[b].name, L.s[a].name)) std::swap(a, b);
      L.parent[b] = a;
    }
  }
  for (auto& tl : g_tl)
    for (u32 s : tl.fresh) L.s[s].name = L.s[L.find(s)].name;
}

// ---------------------------------------------------------------- phase 2
struct DedupScratch {
  std::vector<EKey> a;
  std::vector<u32> tab;
};

static inline void mark_loser(u32 ref) {
  if (ref != NOREF) g_loser[ref >> 6].fetch_or(1ULL << (ref & 63), std::memory_order_relaxed);
}

static void dedup_partition(int tid, u32 p, DedupScratch& S) {
  STEP_BEGIN
  size_t n = 0;
  for (int t = 0; t < NT; t++) n += g_keys.count(t, p);
  if (n < 2) return;
  S.a.resize(n);
  size_t k = 0;
  for (int t = 0; t < NT; t++) g_keys.each(t, p, [&](const EKey& x) { S.a[k++] = x; });
  STEP(5)
  size_t cap = 16;
  int lg = 4;
  while (cap < n + n / 2) { cap <<= 1; lg++; }
  if (S.tab.size() < cap) S.tab.resize(cap);
  u32* tab = S.tab.data();
  memset(tab, 0, cap * sizeof(u32));
  u64 mask = cap - 1;
  int sh = 64 - lg;
  const EKey* A = S.a.data();
  for (u32 i = 0; i < n; i++) {
    const EKey& x = A[i];
    u64 h = hmix(x.eid, K3) >> sh;
    while (true) {
      u32 t = tab[h];
      if (!t) {
        tab[h] = i + 1;
        break;
      }
      const EKey& w = A[t - 1];
      if (w.eid == x.eid) {
        if (x.ing < w.ing) {
          mark_loser(w.ref);
          tab[h] = i + 1;
        } else
          mark_loser(x.ref);
        break;
      }
      h = (h + 1) & mask;
    }
  }
  STEP(6)
}

static void map_partition(int tid, u32 u) {
  STEP_BEGIN
  const u64* lw = (const u64*)g_loser;
  (void)lw;
  for (int t = 0; t < NT; t++) {
#ifndef PARSE_LOOKUP
    g_pay.runs(t, u, [&](const EPay* a, const EPay* b) {
      u32 r = g_pay.ref(a);
      for (; a != b; a++, r++) {
        if ((lw[r >> 6] >> (r & 63)) & 1) continue;
        u64 person = g_links.lookup(a->user);
        g_rec2.put(tid, p2_of(person), Rec2{person, a->ts, a->pc});
      }
    });
#endif
    STEP(7)
    g_craw.each(t, u, [&](const ClickRec& c0) {
      ClickRec c = c0;
      c.who = g_links.lookup(c.who);
      g_cper.push(tid, p2_of(c.who), c);
    });
    STEP(8)
  }
}

// ---------------------------------------------------------------- phase 3: sessions
struct PEnt {
  u64 key;
  u32 cnt, off, ccnt, coff;
};
struct ClickE {
  u32 ts;
  u32 camp;
  u64 cid;
};
struct SessScratch {
  std::vector<u32> tab;
  std::vector<PEnt> ents;
  std::vector<u32> eidx;
  std::vector<Rec2> in;
  std::vector<u64> ev;
  std::vector<u32> cidx;
  std::vector<ClickE> cl;
  std::vector<u32> ccnt, cpos, clist, center;
  std::vector<i64> catsum;
  std::vector<u32> cattouch;
  std::vector<u64> tmp;
  std::vector<u64> tk;
  std::vector<u32> pcs;
  std::vector<ClickE> cin;
};

// Sort u64 keys by their top 32 bits (stable), LSD radix with 11-bit digits.
static void radix_hi32(u64* a, size_t n, std::vector<u64>& tmp) {
  if (n < 64) {
    for (size_t i = 1; i < n; i++) {
      u64 x = a[i];
      size_t j = i;
      while (j > 0 && (a[j - 1] >> 32) > (x >> 32)) {
        a[j] = a[j - 1];
        j--;
      }
      a[j] = x;
    }
    return;
  }
  if (tmp.size() < n) tmp.resize(n);
  static thread_local u32 cnt[3][2048];
  memset(cnt, 0, sizeof cnt);
  for (size_t i = 0; i < n; i++) {
    u64 x = a[i];
    cnt[0][(x >> 32) & 2047]++;
    cnt[1][(x >> 43) & 2047]++;
    cnt[2][(x >> 54) & 1023]++;
  }
  u64* src = a;
  u64* dst = tmp.data();
  for (int pass = 0; pass < 3; pass++) {
    u32* c = cnt[pass];
    int sh = 32 + pass * 11;
    u32 nb = pass == 2 ? 1024 : 2048;
    if (c[(src[0] >> sh) & (nb - 1)] == n) continue;  // all in one bucket: already ordered on this digit
    u32 s0 = 0;
    for (u32 b = 0; b < nb; b++) {
      u32 x = c[b];
      c[b] = s0;
      s0 += x;
    }
    for (size_t i = 0; i < n; i++) {
      u64 x = src[i];
      dst[c[(x >> sh) & (nb - 1)]++] = x;
    }
    std::swap(src, dst);
  }
  if (src != a) memcpy(a, src, n * sizeof(u64));
}

__attribute__((always_inline)) static inline bool click_less(const ClickE& a, const ClickE& b) {
  if (a.ts != b.ts) return a.ts < b.ts;
  if (!((a.cid | b.cid) & LONGBIT)) return a.cid < b.cid;
  return key_less(a.cid, b.cid);
}

static void session_partition(int tid, u32 q, SessScratch& S) {
  STEP_BEGIN
  TL& me = g_tl[tid];
  size_t n = 0;
  for (int t = 0; t < NT; t++) n += g_rec2.count(t, q);
  if (!n) return;
  size_t cap = 64;
  int lg = 6;
  while (cap < n / 4) { cap <<= 1; lg++; }
  S.tab.assign(cap, 0);
  S.ents.clear();
  S.eidx.resize(n);
  S.tk.resize(n);
  S.pcs.resize(n);
  u64 mask = cap - 1;
  int sh = 64 - lg;
  auto rehash = [&]() {
    cap <<= 1;
    lg++;
    mask = cap - 1;
    sh = 64 - lg;
    S.tab.assign(cap, 0);
    for (u32 e = 0; e < S.ents.size(); e++) {
      u64 i = hmix(S.ents[e].key, K3) >> sh;
      while (S.tab[i]) i = (i + 1) & mask;
      S.tab[i] = e + 1;
    }
  };
  size_t k0 = 0;
  const u64* lw = (const u64*)g_loser;
  (void)lw;
  for (int t = 0; t < NT; t++) g_rec2.runs(t, q, [&](const Rec2* ra, const Rec2* rb) {
   u32 rr = g_rec2.ref(ra);
   for (; ra != rb; ra++, rr++) {
#ifdef PARSE_LOOKUP
    if ((lw[rr >> 6] >> (rr & 63)) & 1) continue;
#endif
    const Rec2& r = *ra;
    size_t k = k0++;
    u64 i = hmix(r.person, K3) >> sh;
    u32 e;
    while (true) {
      u32 t = S.tab[i];
      if (!t) {
        e = (u32)S.ents.size();
        S.ents.push_back({r.person, 0, 0, 0, 0});
        S.tab[i] = e + 1;
        if (S.ents.size() * 2 > cap) rehash();
        break;
      }
      if (S.ents[t - 1].key == r.person) {
        e = t - 1;
        break;
      }
      i = (i + 1) & mask;
    }
    S.ents[e].cnt++;
    S.eidx[k] = e;
    S.tk[k] = ((u64)r.ts << 32) | (u64)k;
    S.pcs[k] = r.pc;
   }
  });
  n = k0;
  STEP(0)
  u32 off = 0;
  for (auto& e : S.ents) {
    e.off = off;
    off += e.cnt;
    e.cnt = 0;
  }
  S.ev.resize(n);
  STEP(1)
#ifdef SESS_PERSORT
  for (size_t k = 0; k < n; k++) {
    PEnt& e = S.ents[S.eidx[k]];
    S.ev[e.off + e.cnt++] = (S.tk[k] & 0xFFFFFFFF00000000ULL) | S.pcs[k];
  }
  for (auto& e : S.ents) {
    u64* ev = S.ev.data() + e.off;
    u32 m = e.cnt;
    if (m < 2) continue;
    if (m < 24) {
      for (u32 i = 1; i < m; i++) {
        u64 x = ev[i];
        u32 j = i;
        while (j > 0 && ev[j - 1] > x) {
          ev[j] = ev[j - 1];
          j--;
        }
        ev[j] = x;
      }
    } else
      std::sort(ev, ev + m);
  }
#else
  radix_hi32(S.tk.data(), n, S.tmp);
  for (size_t i = 0; i < n; i++) {
    u64 x = S.tk[i];
    u32 k = (u32)x;
    PEnt& e = S.ents[S.eidx[k]];
    S.ev[e.off + e.cnt++] = (x & 0xFFFFFFFF00000000ULL) | S.pcs[k];
  }
#endif
  STEP(2)
  // clicks
  size_t nc = 0;
  for (int t = 0; t < NT; t++) nc += g_cper.count(t, q);
  S.cidx.resize(nc);
  S.cin.resize(nc);
  if (S.tk.size() < nc) S.tk.resize(nc);
  size_t k = 0;
  for (int t = 0; t < NT; t++)
    g_cper.each(t, q, [&](const ClickRec& c) {
      u64 i = hmix(c.who, K3) >> sh;
      u32 e = UINT32_MAX;
      while (true) {
        u32 t = S.tab[i];
        if (!t) break;
        if (S.ents[t - 1].key == c.who) {
          e = t - 1;
          break;
        }
        i = (i + 1) & mask;
      }
      if (e != UINT32_MAX) {
        S.ents[e].ccnt++;
        S.cidx[k] = e;
        S.cin[k] = {c.ts, c.camp, c.cid};
        S.tk[k] = ((u64)c.ts << 32) | (u64)k;
        k++;
      }
    });
  off = 0;
  for (auto& e : S.ents) {
    e.coff = off;
    off += e.ccnt;
    e.ccnt = 0;
  }
  S.cl.resize(off);
  {
    size_t m = k;
    radix_hi32(S.tk.data(), m, S.tmp);
    for (size_t i = 0; i < m; i++) {
      u32 kk = (u32)S.tk[i];
      PEnt& e = S.ents[S.cidx[kk]];
      S.cl[e.coff + e.ccnt++] = S.cin[kk];
    }
  }

  STEP(3)
  const u32 ncamp = (u32)g_camp_id.size();
  if (S.ccnt.size() < ncamp) {
    S.ccnt.assign(ncamp, 0);
    S.cpos.assign(ncamp, 0);
    S.center.assign(ncamp, 0);
  }
  if (S.catsum.size() < g_cats.size()) S.catsum.assign(g_cats.size(), 0);
  const size_t NC = g_camp_id.size();
  u32* as = me.a_sess.data();
  u32* ap = me.a_purch.data();
  u32* aa = me.a_assist.data();
  i64* ar = me.a_rev.data();
  const int ND = NDAYS;
  const i64 dayoff = WS - DAY0 * DAYMS;

  for (auto& e : S.ents) {
    u64* ev = S.ev.data() + e.off;
    u32 m = e.cnt;
    ClickE* cl = S.cl.data() + e.coff;
    u32 kc = e.ccnt;
#ifdef EXP_WALK_NOCLICK
    kc = 0;
#endif
    if (kc > 1) {
      // sorted by ts; order equal-ts runs by click_id and drop duplicate copies
      u32 w = 0;
      for (u32 i = 0; i < kc;) {
        u32 j = i + 1;
        while (j < kc && cl[j].ts == cl[i].ts) j++;
        if (j - i > 1) std::sort(cl + i, cl + j, [](const ClickE& a, const ClickE& b) { return click_less(a, b); });
        for (u32 x = i; x < j; x++)
          if (x == i || cl[x].cid != cl[x - 1].cid) cl[w++] = cl[x];
        i = j;
      }
      kc = w;
    }
    u32 lo = 0, hi = 0;
    S.clist.clear();
    i32 cur_day = -1;
    // assists: a campaign in the click window assists every session counted while it is
    // there; credit it lazily when it leaves the window or the day changes.
    u32 cnt_day = 0;
    u32* cen = S.center.data();
    auto flush_assists = [&]() {
      if (cur_day < 0) return;
      u32* ad = aa + (size_t)cur_day * NC;
      for (u32 c : S.clist) {
        ad[c] += cnt_day - cen[c];
        cen[c] = 0;
      }
    };
    i64 d_sess = 0, d_ev = 0, d_rev = 0;
    S.cattouch.clear();
    auto flush_day = [&]() {
      if (cur_day < 0) return;
      u32 top = UINT32_MAX;
      i64 best = -1;
      for (u32 c : S.cattouch) {
        i64 v = S.catsum[c];
        if (v > best || (v == best && c < top)) {
          best = v;
          top = c;
        }
        S.catsum[c] = 0;
      }
      S.cattouch.clear();
#ifndef EXP_WALK_NOROW
      me.rows.push_back({e.key, (i32)(DAY0 + cur_day), top, d_sess, d_ev, d_rev});
#endif
      d_sess = d_ev = d_rev = 0;
    };
    u32 i = 0;
    while (i < m) {
      u32 s = (u32)(ev[i] >> 32);
      u32 prev = s;
      i64 rev = 0;
      bool purch = false;
      i32 day = (i32)((dayoff + (i64)s) / DAYMS);
      if (day != cur_day) {
        flush_day();
        flush_assists();
        cur_day = day;
        cnt_day = 0;
      }
      u32 j = i;
      while (j < m) {
        u32 t = (u32)(ev[j] >> 32);
        if (t - prev > (u32)GAPMS) break;
        prev = t;
        u32 pc = (u32)ev[j];
        if (pc) {
          u32 price = pc & 0xFFFFFF, cat = pc >> 24;
          rev += price;
          purch = true;
          if (S.catsum[cat] == 0) S.cattouch.push_back(cat);
          S.catsum[cat] += price;
        }
        j++;
      }
      d_sess++;
      d_ev += j - i;
      d_rev += rev;
      u64 sc = (u64)s + (u64)WEEKMS;
      while (hi < kc && cl[hi].ts <= sc) {
        u32 c = cl[hi].camp;
        if (S.ccnt[c]++ == 0) {
          S.cpos[c] = (u32)S.clist.size();
          S.clist.push_back(c);
          cen[c] = cnt_day;
        }
        hi++;
      }
      while (lo < hi && cl[lo].ts < s) {
        u32 c = cl[lo].camp;
        if (--S.ccnt[c] == 0) {
          aa[(size_t)cur_day * NC + c] += cnt_day - cen[c];
          cen[c] = 0;
          u32 pos = S.cpos[c];
          u32 last = S.clist.back();
          S.clist[pos] = last;
          S.cpos[last] = pos;
          S.clist.pop_back();
        }
        lo++;
      }
      u32 credited = 0;
      if (hi > lo) {
        u32 jj = hi - 1;
        u32 t = cl[jj].ts;
        while (jj > lo && cl[jj - 1].ts == t) jj--;
        credited = cl[jj].camp;
      }
      size_t di = (size_t)day * NC;
      as[di + credited]++;
      ap[di + credited] += purch;
      ar[di + credited] += rev;
      cnt_day++;
      i = j;
    }
    flush_day();
    flush_assists();
    for (u32 c : S.clist) S.ccnt[c] = 0;
    S.clist.clear();
  }
  STEP(4)
}

// ---------------------------------------------------------------- output
static void write_user_file(const std::string& path, std::vector<UserRow>& rows) {
  std::vector<pq::Column> cols(6);
  cols[0] = {"user_id", pq::BYTE_ARRAY, pq::L_STRING, false, {}, {}};
  cols[1] = {"day", pq::INT32, pq::L_DATE, false, {}, {}};
  cols[2] = {"sessions", pq::INT64, pq::L_NONE, false, {}, {}};
  cols[3] = {"events", pq::INT64, pq::L_NONE, false, {}, {}};
  cols[4] = {"revenue_cents", pq::INT64, pq::L_NONE, false, {}, {}};
  cols[5] = {"top_category", pq::BYTE_ARRAY, pq::L_STRING, true, {}, {}};
  size_t n = rows.size();
  {
    std::string& s = cols[0].values;
    size_t bound = 0;
    for (auto& r : rows) bound += 4 + ((r.name & LONGBIT) ? g_istr[(u32)(r.name & 0xffffffffu)].size() : 9);
    s.resize(bound);
    char* o = &s[0];
    for (auto& r : rows) {
      u32 len = decode_fast(r.name, o + 4);
      memcpy(o, &len, 4);
      o += 4 + len;
    }
    s.resize((size_t)(o - &s[0]));
  }
  {
    std::string& s = cols[1].values;
    s.resize(n * 4);
    char* o = &s[0];
    for (auto& r : rows) {
      memcpy(o, &r.day, 4);
      o += 4;
    }
  }
  for (int c = 0; c < 3; c++) {
    std::string& s = cols[2 + c].values;
    s.resize(n * 8);
    char* o = &s[0];
    for (auto& r : rows) {
      i64 v = c == 0 ? r.sessions : c == 1 ? r.events : r.rev;
      memcpy(o, &v, 8);
      o += 8;
    }
  }
  {
    pq::DefLevels dl;
    std::string& s = cols[5].values;
    for (auto& r : rows) {
      if (r.topcat == UINT32_MAX)
        dl.add(0);
      else {
        dl.add(1);
        const std::string& c = g_cats[r.topcat];
        pq::put_str(s, c.data(), (u32)c.size());
      }
    }
    cols[5].deflev = dl.finish();
  }
  if (!pq::write_file(path, (int64_t)n, cols)) {
    fprintf(stderr, "failed to write %s\n", path.c_str());
    exit(1);
  }
}

static void write_campaign_file(const std::string& path, const std::vector<CD>& cd) {
  std::vector<pq::Column> cols(7);
  cols[0] = {"campaign_id", pq::BYTE_ARRAY, pq::L_STRING, false, {}, {}};
  cols[1] = {"channel", pq::BYTE_ARRAY, pq::L_STRING, false, {}, {}};
  cols[2] = {"day", pq::INT32, pq::L_DATE, false, {}, {}};
  cols[3] = {"sessions", pq::INT64, pq::L_NONE, false, {}, {}};
  cols[4] = {"purchasing_sessions", pq::INT64, pq::L_NONE, false, {}, {}};
  cols[5] = {"revenue_cents", pq::INT64, pq::L_NONE, false, {}, {}};
  cols[6] = {"assisted_sessions", pq::INT64, pq::L_NONE, false, {}, {}};
  int64_t n = 0;
  for (size_t c = 0; c < g_camp_id.size(); c++)
    for (int d = 0; d < NDAYS; d++) {
      const CD& x = cd[c * NDAYS + d];
      if (!x.sessions && !x.assisted) continue;
      n++;
      pq::put_str(cols[0].values, g_camp_id[c].data(), (u32)g_camp_id[c].size());
      pq::put_str(cols[1].values, g_camp_ch[c].data(), (u32)g_camp_ch[c].size());
      pq::put_i32(cols[2].values, (int32_t)(DAY0 + d));
      pq::put_i64(cols[3].values, x.sessions);
      pq::put_i64(cols[4].values, x.purch);
      pq::put_i64(cols[5].values, x.rev);
      pq::put_i64(cols[6].values, x.assisted);
    }
  if (!pq::write_file(path, n, cols)) {
    fprintf(stderr, "failed to write %s\n", path.c_str());
    exit(1);
  }
}

static size_t RDBUF = 256 << 10;

// Read chunk [beg, end) of a file with pread into a reusable buffer and parse every line
// that starts inside it.
static void process_chunk(int tid, const Chunk& c, std::vector<char>& vb) {
  const FileMap& fi = g_files[c.file];
#ifdef USE_MMAP
  {
    (void)vb;
    const char* base = fi.map;
    const char* fe = base + fi.size;
    const char* p = base + c.beg;
    if (c.beg == 0) {
      const char* nl = (const char*)memchr(p, '\n', fi.size);
      p = nl ? nl + 1 : fe;
    } else if (base[c.beg - 1] != '\n') {
      const char* nl = (const char*)memchr(p, '\n', (size_t)(fe - p));
      p = nl ? nl + 1 : fe;
    }
    const char* cend = base + c.end;
    if (p >= cend) return;
    if (fi.kind == 0) parse_events(tid, p, cend, fe, true);
    else if (fi.kind == 1) parse_clicks(tid, p, cend, fe, true);
    else parse_links(tid, p, cend, fe, true);
    return;
  }
#endif
  size_t cap = vb.size() - 512;
  char* buf = vb.data();
  size_t boff = c.beg == 0 ? 0 : c.beg - 1;  // file offset of buf[0]
  size_t roff = boff;
  size_t keep = 0;
  bool skip = true;  // skip the header (beg == 0) or up to the first line start >= beg
  while (true) {
    if (keep == cap) {
      vb.resize(cap * 2 + 512);
      buf = vb.data();
      cap = vb.size() - 512;
    }
    size_t want = std::min(cap - keep, fi.size - roff);
    ssize_t n = want ? pread(fi.fd, buf + keep, want, (off_t)roff) : 0;
    if (n < 0) {
      perror("pread");
      exit(1);
    }
    roff += (size_t)n;
    size_t len = keep + (size_t)n;
    bool eof = roff >= fi.size || n == 0;
    const char* p = buf;
    const char* fend = buf + len;
    if (skip) {
      const char* nl = (const char*)memchr(p, '\n', len);
      if (!nl) {
        if (eof) return;
        keep = 0;
        boff = roff;
        continue;
      }
      p = nl + 1;
      skip = false;
    }
    const char* cend = (c.end - boff <= len) ? buf + (c.end - boff) : fend + 1;
    if (p >= cend) return;
    const char* r;
    if (fi.kind == 0)
      r = parse_events(tid, p, cend, fend, eof);
    else if (fi.kind == 1)
      r = parse_clicks(tid, p, cend, fend, eof);
    else
      r = parse_links(tid, p, cend, fend, eof);
    if (eof || r >= cend) return;
    keep = (size_t)(fend - r);
    memmove(buf, r, keep);
    boff += (size_t)(r - buf);
  }
}

template <class F>
static void parallel(int nt, F fn) {
  std::vector<std::thread> th;
  for (int t = 1; t < nt; t++) th.emplace_back(fn, t);
  fn(0);
  for (auto& x : th) x.join();
}

static i64 floordiv(i64 a, i64 b) { return a / b - ((a % b != 0) && ((a < 0) != (b < 0))); }

int main() {
  T0 = now_s();
  umask(0);  // outputs deletable by whoever runs the container
  init_dates();
  const char* e;
  std::string DATA = (e = getenv("DATA_DIR")) ? e : "/data";
  std::string OUT = (e = getenv("OUTPUT_DIR")) ? e : "/output";
  NT = (e = getenv("NCPU")) ? atoi(e) : (int)std::thread::hardware_concurrency();
  if (NT < 1) NT = 1;
  if (NT > 64) NT = 64;

  {
    std::string m = read_file(DATA + "/manifest.json");
    std::string a, b;
    a.reserve(64);
    b.reserve(64);
    if (!find_json_str(m, "window_start", a) || !find_json_str(m, "window_end", b)) {
      fprintf(stderr, "bad manifest\n");
      return 1;
    }
    a.resize(a.size() + 32, '\0');
    b.resize(b.size() + 32, '\0');
    if (!parse_ts(a.data(), (u32)(a.size() - 32), WS) || !parse_ts(b.data(), (u32)(b.size() - 32), WE)) {
      fprintf(stderr, "bad manifest times\n");
      return 1;
    }
    CB = WS - WEEKMS;
    if (WE - CB >= (i64)UINT32_MAX || WE <= WS) {
      fprintf(stderr, "unsupported window\n");
      return 1;
    }
    DAY0 = floordiv(WS, DAYMS);
    NDAYS = (int)(floordiv(WE - 1, DAYMS) - DAY0 + 1);
  }
  {
    std::string s = read_file(DATA + "/products.csv");
    std::vector<std::pair<u64, u32>> prods;  // code, first-seen category index
    std::unordered_map<std::string, u32> seen;
    std::vector<std::string> names;
    std::string last;
    u32 lastid = 0;
    each_line<2>(s, [&](Fld* f) {
      if (f[0].n == 0) return;
      char buf[32];
      u64 code;
      if (f[0].n <= 9) {
        memset(buf, 0, sizeof buf);
        memcpy(buf, f[0].p, f[0].n);
        code = pack7(buf, f[0].n);
      } else
        code = intern_slow(f[0].p, f[0].n);
      u32 id;
      if (!names.empty() && last.size() == f[1].n && memcmp(last.data(), f[1].p, f[1].n) == 0)
        id = lastid;
      else {
        std::string c(f[1].p, f[1].n);
        auto it = seen.find(c);
        if (it == seen.end()) {
          id = (u32)names.size();
          seen.emplace(c, id);
          names.push_back(c);
        } else
          id = it->second;
        last = c;
        lastid = id;
      }
      prods.push_back({code, id});
    });
    std::vector<std::string> cats = names;
    if (!seen.count("unknown")) cats.push_back("unknown");
    std::sort(cats.begin(), cats.end());
    if (cats.size() > 255) {
      fprintf(stderr, "too many categories\n");
      return 1;
    }
    g_cats = cats;
    std::vector<u32> remap(names.size());
    for (u32 i = 0; i < names.size(); i++)
      remap[i] = (u32)(std::lower_bound(cats.begin(), cats.end(), names[i]) - cats.begin());
    g_unknown_cat = (u32)(std::lower_bound(cats.begin(), cats.end(), std::string("unknown")) - cats.begin());
    g_prod.init(prods.size());
    for (auto& pr : prods) {
      u32 tmp;
      if (!g_prod.get(pr.first, tmp)) g_prod.put(pr.first, remap[pr.second]);
    }
  }
  {
    std::string s = read_file(DATA + "/campaigns.csv");
    g_camp_id.push_back("organic");
    g_camp_ch.push_back("organic");
    std::vector<std::pair<std::string, std::string>> cs;
    each_line<2>(s, [&](Fld* f) {
      if (f[0].n == 0) return;
      cs.push_back({std::string(f[0].p, f[0].n), std::string(f[1].p, f[1].n)});
    });
    g_camp.init(cs.size());
    for (auto& c : cs) {
      u32 tmp;
      u64 code = encode_str(c.first);
      if (g_camp.get(code, tmp)) continue;
      g_camp.put(code, (u32)g_camp_id.size());
      g_camp_id.push_back(c.first);
      g_camp_ch.push_back(c.second);
    }
  }
  size_t ev_bytes = 0, link_bytes = 0, click_bytes = 0;
  for (auto& f : list_csv(DATA + "/links")) open_file(f, 2);
  for (auto& f : list_csv(DATA + "/events")) open_file(f, 0);
  for (auto& f : list_csv(DATA + "/clicks")) open_file(f, 1);
  for (auto& f : g_files) {
    if (f.kind == 0) ev_bytes += f.size;
    if (f.kind == 2) link_bytes += f.size;
    if (f.kind == 1) click_bytes += f.size;
  }
  size_t ev_est = ev_bytes / 74 + 1024;
  PBITS = 4;
  #ifndef PART_TARGET
#define PART_TARGET 65536
#endif
  while ((1ull << PBITS) * (u64)PART_TARGET < ev_est && PBITS < 12) PBITS++;
  P = 1 << PBITS;
#ifndef P1_TARGET
#define P1_TARGET PART_TARGET
#endif
#ifndef P2_TARGET
#define P2_TARGET PART_TARGET
#endif
  P1BITS = 4;
  while ((1ull << P1BITS) * (u64)P1_TARGET < ev_est && P1BITS < 13) P1BITS++;
  P1 = 1 << P1BITS;
  P2BITS = 4;
  while ((1ull << P2BITS) * (u64)P2_TARGET < ev_est && P2BITS < 13) P2BITS++;
  P2 = 1 << P2BITS;
  g_links.init(link_bytes / 10 + 1024);
  size_t cap_per = ev_est / ((size_t)NT * P);
  size_t cap1 = ev_est / ((size_t)NT * P1);
  cap1 = cap1 + cap1 / 16 + 32;
  size_t cap2 = ev_est / ((size_t)NT * P2);
  cap2 = cap2 + cap2 / 16 + 32;
  cap_per = cap_per + cap_per / 16 + 32;
  g_keys.init(cap1, NT, P1);
#ifdef PARSE_LOOKUP
  g_rec2.init(cap2, NT, P2);
  g_loser = (std::atomic<u64>*)big_alloc((g_rec2.total_n / 64 + 1) * sizeof(u64));
#else
  g_pay.init(cap_per, NT, P);
  g_loser = (std::atomic<u64>*)big_alloc((g_pay.total_n / 64 + 1) * sizeof(u64));
#endif
  size_t click_est = click_bytes / 44 + 1024;
  size_t ccap = click_est / ((size_t)NT * P);
  ccap = ccap + ccap / 16 + 16;
  g_craw.init(ccap, NT, P);
  g_cper.init(click_est / ((size_t)NT * P2) + click_est / ((size_t)NT * P2) / 16 + 16, NT, P2);
  logt("setup");

  std::vector<Chunk> chunks;
  size_t CH = 4 << 20;
  if ((e = getenv("SS_CHUNK")) && atol(e) > 0) CH = (size_t)atol(e);
  if ((e = getenv("SS_RDBUF")) && atol(e) > 0) RDBUF = (size_t)atol(e);
  int nlinkchunks = 0;
  for (int kind : {2, 1, 0}) {
    for (int fi = 0; fi < (int)g_files.size(); fi++) {
      if (g_files[fi].kind != kind) continue;
      size_t n = g_files[fi].size;
      for (size_t b = 0; b < n; b += CH) {
        chunks.push_back({fi, b, std::min(n, b + CH)});
        if (kind == 2) nlinkchunks++;
      }
    }
  }
  g_tl.resize(NT);
  std::atomic<size_t> next_chunk{0};
  std::atomic<int> links_left{nlinkchunks};
  static std::atomic<bool> uf_done{false};
  if (nlinkchunks == 0) {
    run_union_find();
    uf_done.store(true);
  }
  parallel(NT, [&](int tid) {
    std::vector<char> vb(RDBUF + 512);
    while (true) {
      size_t ci = next_chunk.fetch_add(1);
      if (ci >= chunks.size()) break;
#ifdef PARSE_LOOKUP
      if (g_files[chunks[ci].file].kind == 0)
        while (!uf_done.load(std::memory_order_acquire)) std::this_thread::yield();
#endif
      process_chunk(tid, chunks[ci], vb);
      if (g_files[chunks[ci].file].kind == 2 && links_left.fetch_sub(1) == 1) {
        run_union_find();
        uf_done.store(true, std::memory_order_release);
      }
    }
#ifdef PARSE_LOOKUP
    flush_pend(tid, g_tl[tid]);
    g_rec2.finish(tid);
#endif
    g_keys.finish(tid);
    g_pay.finish(tid);
  });
  logt("parse");

  // phase 2a: dedup
  {
    std::atomic<u32> nx{0};
    parallel(NT, [&](int tid) {
      DedupScratch S;
      while (true) {
        u32 p = nx.fetch_add(1);
        if (p >= (u32)P1) break;
        dedup_partition(tid, p, S);
      }
    });
  }
  logt("dedup");
  // phase 2b: map persons, scatter by person
  {
#ifndef PARSE_LOOKUP
    if (PartBuf<Rec2>::total_for(cap2, NT, P2) <= g_keys.total_n)
      g_rec2.init(cap2, NT, P2, g_keys.base, g_keys.total_n);  // keys are dead: reuse their memory
    else
      g_rec2.init(cap2, NT, P2);
#endif
    std::atomic<u32> nx{0};
    parallel(NT, [&](int tid) {
      while (true) {
        u32 u = nx.fetch_add(1);
        if (u >= (u32)P) break;
        map_partition(tid, u);
      }
      g_rec2.finish(tid);
    });
  }
  logt("map");

  // phase 3 (+ output: each thread writes its own user_daily file as soon as it is done;
  // the last thread to finish merges and writes campaign_daily)
  std::string UD = OUT + "/user_daily", CDD = OUT + "/campaign_daily";
  mkdir(OUT.c_str(), 0777);
  mkdir(UD.c_str(), 0777);
  mkdir(CDD.c_str(), 0777);
  {
    std::vector<std::pair<size_t, u32>> order;
    for (u32 q = 0; q < (u32)P2; q++) {
      size_t n = 0;
      for (int t = 0; t < NT; t++) n += g_rec2.count(t, q);
      order.push_back({n, q});
    }
    std::sort(order.begin(), order.end(), [](auto& a, auto& b) { return a.first > b.first; });
    std::atomic<size_t> nx{0};
    std::atomic<int> finished{0};
    std::atomic<size_t> total_rows{0};
    parallel(NT, [&](int tid) {
      TL& me = g_tl[tid];
      size_t ncd = g_camp_id.size() * (size_t)NDAYS;
      me.a_sess.assign(ncd, 0);
      me.a_purch.assign(ncd, 0);
      me.a_assist.assign(ncd, 0);
      me.a_rev.assign(ncd, 0);
      {
        SessScratch S;
        while (true) {
          size_t i = nx.fetch_add(1);
          if (i >= order.size()) break;
          session_partition(tid, order[i].second, S);
        }
      }
      char nm[64];
      snprintf(nm, sizeof nm, "/part-%05d.parquet", tid);
      if (!me.rows.empty()) write_user_file(UD + nm, me.rows);
      total_rows.fetch_add(me.rows.size());
      if (finished.fetch_add(1, std::memory_order_acq_rel) == NT - 1) {
        if (total_rows.load() == 0) write_user_file(UD + nm, me.rows);
        std::vector<CD> total(g_camp_id.size() * (size_t)NDAYS, CD{0, 0, 0, 0});
        const size_t NCm = g_camp_id.size();
        for (auto& tl : g_tl)
          for (int d = 0; d < NDAYS; d++)
            for (size_t c = 0; c < NCm; c++) {
              size_t j = (size_t)d * NCm + c;
              CD& t = total[c * NDAYS + d];
              t.sessions += tl.a_sess[j];
              t.purch += tl.a_purch[j];
              t.rev += tl.a_rev[j];
              t.assisted += tl.a_assist[j];
            }
        write_campaign_file(CDD + "/part-00000.parquet", total);
      }
    });
  }
  logt("sessions");
  logt("write");
#ifdef PROF_STEPS
  {
    const char* nm[] = {"sess.passA", "sess.prep", "sess.ev_sort_place", "sess.clicks", "sess.walk", "dedup.copy",
                        "dedup.hash", "map.pay", "map.clicks"};
    for (int i = 0; i < 9; i++) {
      double t = 0;
      for (int k = 0; k < NT; k++) t += g_st[k][i];
      fprintf(stderr, "STEP %-20s %.3f s cpu (%.3f wall-eq)\n", nm[i], t, t / NT);
    }
  }
#endif
  fflush(stderr);
  _exit(0);
}
