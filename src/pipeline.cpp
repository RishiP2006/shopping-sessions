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

static u64 intern_slow(const char* p, u32 n) {
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
static inline u64 pack7(const char* p, u32 n) {
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

static bool key_less(u64 a, u64 b) {
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
static inline bool civil_core(const char* p, i64& out) {
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

// p[0..n+15] readable
static inline bool parse_ts(const char* p, u32 n, i64& out) {
  if (n == 20) {
    if (p[4] == '-') return civil_core<0xFF0000FFu, 0x5A00003Au>(p, out);
  } else if (n == 25) {
    if (p[4] != '-') goto digits;
    i64 base;
    if (!civil_core<0x000000FFu, 0x0000003Au>(p, base)) return false;
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

// Scan lines starting at p (a line start) while the line start is < cend.
// fend is the end of the file; >= 64 readable bytes follow it.
template <int N, class F>
static void scan(const char* p, const char* cend, const char* fend, F&& fn) {
  Fld f[N];
  u32 nf = 0;
  const char* fs = p;
  const char* blk = p;
  u32 qacc = 0, qline0 = 0;
  auto finish = [&](u32 qnow) {
    if (nf == N) {
      u32 ql = qnow - qline0;
      if (UNLIKELY(ql != 0)) {
        u32 pairs = 0;
        for (int i = 0; i < N; i++) {
          if (f[i].n >= 2 && f[i].p[0] == '"' && f[i].p[f[i].n - 1] == '"') {
            pairs++;
            f[i].p++;
            f[i].n -= 2;
          }
        }
        if (pairs * 2 != ql) return;
      }
      fn(f);
    }
  };
  while (blk < fend) {
    u64 C, Q, NL;
    masks64(blk, C, Q, NL);
    size_t rem = (size_t)(fend - blk);
    if (UNLIKELY(rem < 64)) {
      u64 vm = (1ULL << rem) - 1;
      C &= vm;
      Q &= vm;
      NL &= vm;
    }
    u64 S = C | NL;
    while (S) {
      u32 pos = (u32)__builtin_ctzll(S);
      S &= S - 1;
      const char* e = blk + pos;
      if (LIKELY(nf < (u32)N)) {
        f[nf].p = fs;
        f[nf].n = (u32)(e - fs);
      }
      nf++;
      fs = e + 1;
      if ((NL >> pos) & 1) {
        u32 qnow = qacc + (u32)__builtin_popcountll(Q & ((1ULL << pos) - 1));
        finish(qnow);
        nf = 0;
        qline0 = qnow;
        if (fs >= cend) return;
      }
    }
    qacc += (u32)__builtin_popcountll(Q);
    blk += 64;
  }
  if (nf > 0 || fs < fend) {
    if (nf < (u32)N) {
      f[nf].p = fs;
      f[nf].n = (u32)(fend - fs);
    }
    nf++;
    finish(qacc);
  }
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
    return m + m / 2 + (size_t)nth * parts * CH + CH;
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

// ---------------------------------------------------------------- globals
static int NT = 8;
static i64 WS, WE, CB;
static i64 DAY0;
static int NDAYS;
static int PBITS, P;
static SmallTab g_prod;
static SmallTab g_camp;
static std::vector<std::string> g_cats;
static u32 g_unknown_cat;
static std::vector<std::string> g_camp_id, g_camp_ch;
static LinkTab g_links;

struct FileMap {
  const char* data;
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

static PartBuf<EKey> g_keys;   // by event_id
static PartBuf<EPay> g_pay;    // by user (same hash bits as the link table)
static PartBuf<Rec2> g_rec2;   // by person (reuses g_keys memory)
static PartBuf<ClickRec> g_craw;  // valid clicks by user
static PartBuf<ClickRec> g_cper;  // valid clicks by person
static std::atomic<u64>* g_loser = nullptr;

struct alignas(64) TL {
  std::vector<std::pair<u32, u32>> links;
  std::vector<u32> fresh;
  std::vector<UserRow> rows;
  std::vector<CD> cd;
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

// Map a file with >= 64 KiB of zero padding after it.
static void map_file(const std::string& path, int kind) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) return;
  struct stat st;
  fstat(fd, &st);
  size_t n = (size_t)st.st_size;
  if (n == 0) {
    close(fd);
    return;
  }
  size_t pg = (size_t)sysconf(_SC_PAGESIZE);
  size_t mapped = (n + pg - 1) / pg * pg;
  size_t total = mapped + (64 << 10);
  void* res = mmap(nullptr, total, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (res == MAP_FAILED) {
    fprintf(stderr, "mmap reserve failed\n");
    exit(1);
  }
  void* m = mmap(res, n, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0);
  close(fd);
  if (m == MAP_FAILED) {
    fprintf(stderr, "mmap %s failed\n", path.c_str());
    exit(1);
  }
  g_files.push_back({(const char*)m, n, kind});
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
static inline u32 p1_of(u64 eid) { return (u32)(hmix(eid, K1) >> (64 - PBITS)); }
static inline u32 p2_of(u64 person) { return (u32)(hmix(person, K2) >> (64 - PBITS)); }

static inline u32 pu_of(u64 user) { return (u32)(hmix(user, K4) >> (64 - PBITS)); }

static void parse_events(int tid, const char* p, const char* cend, const char* fend) {
  const i64 wlen = WE - WS;
  scan<7>(p, cend, fend, [&](Fld* f) {
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
      memcpy(&w4, t, 4);
      if (f[4].n == 4) {
        if (w4 != 0x77656976u /*view*/ && w4 != 0x74726163u /*cart*/) break;
      } else {
        u64 w8;
        memcpy(&w8, t, 8);
        if (f[4].n != 8 || w8 != 0x6573616863727570ULL /*purchase*/) break;
        u32 price;
        if (!parse_price(f[6].p, f[6].n, price)) break;
        u32 cat;
        if (!g_prod.get(pack7(f[5].p, f[5].n), cat)) cat = g_unknown_cat;
        pc = price | (cat << 24);
      }
      u64 user = pack7(f[1].p, f[1].n);
      EPay* py = g_pay.slot(tid, pu_of(user));
      py->user = user;
      py->ts = (u32)ts;
      py->pc = pc;
      k.ref = g_pay.ref(py);
    } while (0);
    g_keys.push(tid, p1_of(k.eid), k);
  });
}

static void parse_clicks(int tid, const char* p, const char* cend, const char* fend) {
  const i64 clen = WE - CB;
  scan<4>(p, cend, fend, [&](Fld* f) {
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

static void parse_links(int tid, const char* p, const char* cend, const char* fend) {
  TL& tl = g_tl[tid];
  scan<2>(p, cend, fend, [&](Fld* f) {
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

static void dedup_partition(u32 p, DedupScratch& S) {
  size_t n = 0;
  for (int t = 0; t < NT; t++) n += g_keys.count(t, p);
  if (n < 2) return;
  S.a.resize(n);
  size_t k = 0;
  for (int t = 0; t < NT; t++) g_keys.each(t, p, [&](const EKey& x) { S.a[k++] = x; });
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
}

static void map_partition(int tid, u32 u) {
  const u64* lw = (const u64*)g_loser;
  for (int t = 0; t < NT; t++) {
    g_pay.runs(t, u, [&](const EPay* a, const EPay* b) {
      u32 r = g_pay.ref(a);
      for (; a != b; a++, r++) {
        if ((lw[r >> 6] >> (r & 63)) & 1) continue;
        u64 person = g_links.lookup(a->user);
        g_rec2.push(tid, p2_of(person), Rec2{person, a->ts, a->pc});
      }
    });
    g_craw.each(t, u, [&](const ClickRec& c0) {
      ClickRec c = c0;
      c.who = g_links.lookup(c.who);
      g_cper.push(tid, p2_of(c.who), c);
    });
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
  std::vector<u32> ccnt, cpos, clist;
  std::vector<i64> catsum;
  std::vector<u32> cattouch;
  std::vector<u64> tmp;
};

static void radix_sort_ts(u64* a, size_t n, std::vector<u64>& tmp) {
  tmp.resize(n);
  u64* src = a;
  u64* dst = tmp.data();
  for (int pass = 0; pass < 3; pass++) {
    int sh = 32 + pass * 11;
    size_t cnt[2048];
    memset(cnt, 0, sizeof cnt);
    for (size_t i = 0; i < n; i++) cnt[(src[i] >> sh) & 2047]++;
    size_t s = 0;
    for (int b = 0; b < 2048; b++) {
      size_t c = cnt[b];
      cnt[b] = s;
      s += c;
    }
    for (size_t i = 0; i < n; i++) dst[cnt[(src[i] >> sh) & 2047]++] = src[i];
    std::swap(src, dst);
  }
  if (src != a) memcpy(a, src, n * sizeof(u64));
}

static inline bool click_less(const ClickE& a, const ClickE& b) {
  if (a.ts != b.ts) return a.ts < b.ts;
  if (!((a.cid | b.cid) & LONGBIT)) return a.cid < b.cid;
  return key_less(a.cid, b.cid);
}

static void session_partition(int tid, u32 q, SessScratch& S) {
  TL& me = g_tl[tid];
  size_t n = 0;
  for (int t = 0; t < NT; t++) n += g_rec2.count(t, q);
  if (!n) return;
  S.in.resize(n);
  {
    size_t k = 0;
    for (int t = 0; t < NT; t++) g_rec2.each(t, q, [&](const Rec2& x) { S.in[k++] = x; });
  }
  size_t cap = 64;
  int lg = 6;
  while (cap < n / 4) { cap <<= 1; lg++; }
  S.tab.assign(cap, 0);
  S.ents.clear();
  S.eidx.resize(n);
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
  for (size_t k = 0; k < n; k++) {
    const Rec2& r = S.in[k];
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
  }
  u32 off = 0;
  for (auto& e : S.ents) {
    e.off = off;
    off += e.cnt;
    e.cnt = 0;
  }
  S.ev.resize(n);
  for (size_t k = 0; k < n; k++) {
    PEnt& e = S.ents[S.eidx[k]];
    S.ev[e.off + e.cnt++] = ((u64)S.in[k].ts << 32) | S.in[k].pc;
  }
  // clicks
  size_t nc = 0;
  for (int t = 0; t < NT; t++) nc += g_cper.count(t, q);
  S.cidx.resize(nc);
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
      S.cidx[k++] = e;
      if (e != UINT32_MAX) S.ents[e].ccnt++;
    });
  off = 0;
  for (auto& e : S.ents) {
    e.coff = off;
    off += e.ccnt;
    e.ccnt = 0;
  }
  S.cl.resize(off);
  k = 0;
  for (int t = 0; t < NT; t++)
    g_cper.each(t, q, [&](const ClickRec& c) {
      u32 ei = S.cidx[k++];
      if (ei == UINT32_MAX) return;
      PEnt& e = S.ents[ei];
      S.cl[e.coff + e.ccnt++] = {c.ts, c.camp, c.cid};
    });

  const u32 ncamp = (u32)g_camp_id.size();
  if (S.ccnt.size() < ncamp) {
    S.ccnt.assign(ncamp, 0);
    S.cpos.assign(ncamp, 0);
  }
  if (S.catsum.size() < g_cats.size()) S.catsum.assign(g_cats.size(), 0);
  CD* cd = me.cd.data();
  const int ND = NDAYS;
  const i64 dayoff = WS - DAY0 * DAYMS;

  for (auto& e : S.ents) {
    u64* ev = S.ev.data() + e.off;
    u32 m = e.cnt;
    if (m > 1) {
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
      } else if (m < 20000)
        std::sort(ev, ev + m);
      else
        radix_sort_ts(ev, m, S.tmp);
    }
    ClickE* cl = S.cl.data() + e.coff;
    u32 kc = e.ccnt;
    if (kc > 1) {
      std::sort(cl, cl + kc, click_less);
      u32 w = 1;
      for (u32 i = 1; i < kc; i++)
        if (cl[i].cid != cl[w - 1].cid) cl[w++] = cl[i];
      kc = w;
    }
    u32 lo = 0, hi = 0;
    S.clist.clear();
    i32 cur_day = -1;
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
      me.rows.push_back({e.key, (i32)(DAY0 + cur_day), top, d_sess, d_ev, d_rev});
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
        cur_day = day;
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
        }
        hi++;
      }
      while (lo < hi && cl[lo].ts < s) {
        u32 c = cl[lo].camp;
        if (--S.ccnt[c] == 0) {
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
      CD& x = cd[(size_t)credited * ND + day];
      x.sessions++;
      x.purch += purch;
      x.rev += rev;
      for (u32 c : S.clist) cd[(size_t)c * ND + day].assisted++;
      i = j;
    }
    flush_day();
    for (u32 c : S.clist) S.ccnt[c] = 0;
    S.clist.clear();
  }
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
    std::vector<std::pair<std::string, std::string>> prods;
    std::vector<std::string> cats;
    each_line<2>(s, [&](Fld* f) {
      if (f[0].n == 0) return;
      prods.push_back({std::string(f[0].p, f[0].n), std::string(f[1].p, f[1].n)});
      cats.push_back(std::string(f[1].p, f[1].n));
    });
    cats.push_back("unknown");
    std::sort(cats.begin(), cats.end());
    cats.erase(std::unique(cats.begin(), cats.end()), cats.end());
    if (cats.size() > 255) {
      fprintf(stderr, "too many categories\n");
      return 1;
    }
    g_cats = cats;
    std::unordered_map<std::string, u32> ci;
    for (u32 i = 0; i < cats.size(); i++) ci[cats[i]] = i;
    g_unknown_cat = ci["unknown"];
    g_prod.init(prods.size());
    for (auto& pr : prods) {
      u64 code = encode_str(pr.first);
      u32 tmp;
      if (!g_prod.get(code, tmp)) g_prod.put(code, ci[pr.second]);
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
  for (auto& f : list_csv(DATA + "/links")) map_file(f, 2);
  for (auto& f : list_csv(DATA + "/events")) map_file(f, 0);
  for (auto& f : list_csv(DATA + "/clicks")) map_file(f, 1);
  for (auto& f : g_files) {
    if (f.kind == 0) ev_bytes += f.size;
    if (f.kind == 2) link_bytes += f.size;
    if (f.kind == 1) click_bytes += f.size;
  }
  size_t ev_est = ev_bytes / 74 + 1024;
  PBITS = 4;
  while ((1ull << PBITS) * 16384ull < ev_est && PBITS < 12) PBITS++;
  P = 1 << PBITS;
  g_links.init(link_bytes / 10 + 1024);
  size_t cap_per = ev_est / ((size_t)NT * P);
  cap_per = cap_per + cap_per / 4 + 64;
  g_keys.init(cap_per, NT, P);
  g_pay.init(cap_per, NT, P);
  g_loser = (std::atomic<u64>*)big_alloc((g_pay.total_n / 64 + 1) * sizeof(u64));
  size_t click_est = click_bytes / 44 + 1024;
  size_t ccap = click_est / ((size_t)NT * P);
  ccap = ccap + ccap / 4 + 16;
  g_craw.init(ccap, NT, P);
  g_cper.init(ccap, NT, P);
  logt("setup");

  std::vector<Chunk> chunks;
  const size_t CH = 2 << 20;
  int nlinkchunks = 0;
  for (int kind : {2, 0, 1}) {
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
  if (nlinkchunks == 0) run_union_find();
  parallel(NT, [&](int tid) {
    while (true) {
      size_t ci = next_chunk.fetch_add(1);
      if (ci >= chunks.size()) break;
      Chunk c = chunks[ci];
      const FileMap& fm = g_files[c.file];
      const char* base = fm.data;
      const char* fe = base + fm.size;
      const char* p = base + c.beg;
      if (c.beg == 0) {
        const char* nl = (const char*)memchr(p, '\n', fm.size);
        p = nl ? nl + 1 : fe;
      } else if (base[c.beg - 1] != '\n') {
        const char* nl = (const char*)memchr(p, '\n', (size_t)(fe - p));
        p = nl ? nl + 1 : fe;
      }
      const char* cend = base + c.end;
      if (p < cend) {
        if (fm.kind == 0)
          parse_events(tid, p, cend, fe);
        else if (fm.kind == 1)
          parse_clicks(tid, p, cend, fe);
        else
          parse_links(tid, p, cend, fe);
      }
      if (fm.kind == 2 && links_left.fetch_sub(1) == 1) run_union_find();
    }
  });
  logt("parse");

  // phase 2a: dedup
  {
    std::atomic<u32> nx{0};
    parallel(NT, [&](int tid) {
      DedupScratch S;
      while (true) {
        u32 p = nx.fetch_add(1);
        if (p >= (u32)P) break;
        dedup_partition(p, S);
      }
    });
  }
  logt("dedup");
  // phase 2b: map persons, scatter by person
  {
    g_rec2.init(cap_per, NT, P, g_keys.base, g_keys.total_n);  // keys are dead: reuse their memory
    std::atomic<u32> nx{0};
    parallel(NT, [&](int tid) {
      while (true) {
        u32 u = nx.fetch_add(1);
        if (u >= (u32)P) break;
        map_partition(tid, u);
      }
    });
  }
  logt("map");

  // phase 3
  {
    std::vector<std::pair<size_t, u32>> order;
    for (u32 q = 0; q < (u32)P; q++) {
      size_t n = 0;
      for (int t = 0; t < NT; t++) n += g_rec2.count(t, q);
      order.push_back({n, q});
    }
    std::sort(order.begin(), order.end(), [](auto& a, auto& b) { return a.first > b.first; });
    std::atomic<size_t> nx{0};
    parallel(NT, [&](int tid) {
      TL& me = g_tl[tid];
      me.cd.assign(g_camp_id.size() * (size_t)NDAYS, CD{0, 0, 0, 0});
      SessScratch S;
      while (true) {
        size_t i = nx.fetch_add(1);
        if (i >= order.size()) break;
        session_partition(tid, order[i].second, S);
      }
    });
  }
  logt("sessions");

  std::string UD = OUT + "/user_daily", CDD = OUT + "/campaign_daily";
  mkdir(OUT.c_str(), 0777);
  mkdir(UD.c_str(), 0777);
  mkdir(CDD.c_str(), 0777);
  std::vector<CD> total(g_camp_id.size() * (size_t)NDAYS, CD{0, 0, 0, 0});
  size_t total_rows = 0;
  for (auto& tl : g_tl) total_rows += tl.rows.size();
  parallel(NT, [&](int tid) {
    TL& me = g_tl[tid];
    if (!me.rows.empty() || (tid == 0 && total_rows == 0)) {
      char nm[64];
      snprintf(nm, sizeof nm, "/part-%05d.parquet", tid);
      write_user_file(UD + nm, me.rows);
    }
    if (tid == NT - 1 || NT == 1) {
      for (auto& tl : g_tl)
        for (size_t i = 0; i < total.size(); i++) {
          total[i].sessions += tl.cd[i].sessions;
          total[i].purch += tl.cd[i].purch;
          total[i].rev += tl.cd[i].rev;
          total[i].assisted += tl.cd[i].assisted;
        }
      write_campaign_file(CDD + "/part-00000.parquet", total);
    }
  });
  logt("write");
  fflush(stderr);
  _exit(0);
}
