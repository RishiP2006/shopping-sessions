// Shopping Sessions: native pipeline.
// Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet
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
typedef unsigned __int128 u128;

static const i64 DAYMS = 86400000LL;
static const i64 GAPMS = 1800000LL;
static const i64 WEEKMS = 604800000LL;
static const u64 LONGBIT = 1ULL << 63;

static double now_s() {
  return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
static double T0;
static void logt(const char* what) { fprintf(stderr, "[%7.3f] %s\n", now_s() - T0, what); }

// ---------------------------------------------------------------- string codes
// Strings of length <= 10 over [0-9A-Z_a-z] are packed into 60 bits, order preserving.
// Anything else is interned and gets LONGBIT | id.
static uint8_t SYM[256];
static char UNSYM[64];
static void init_sym() {
  int v = 1;
  for (int c = '0'; c <= '9'; c++) { SYM[c] = v; UNSYM[v] = (char)c; v++; }
  for (int c = 'A'; c <= 'Z'; c++) { SYM[c] = v; UNSYM[v] = (char)c; v++; }
  SYM['_'] = v; UNSYM[v] = '_'; v++;
  for (int c = 'a'; c <= 'z'; c++) { SYM[c] = v; UNSYM[v] = (char)c; v++; }
}

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

static inline u64 encode(const char* p, u32 n) {
  if (n <= 10) {
    u64 c = 0;
    u32 bad = 0;
    for (u32 i = 0; i < n; i++) {
      u32 s = SYM[(uint8_t)p[i]];
      bad |= (s == 0);
      c = (c << 6) | s;
    }
    if (!bad) return c << (6 * (10 - n));
  }
  return intern_slow(p, n);
}

static void decode(u64 code, std::string& out) {
  out.clear();
  if (code & LONGBIT) {
    std::lock_guard<std::mutex> lk(g_imu);
    out = g_istr[(u32)(code & 0xffffffffu)];
    return;
  }
  for (int i = 0; i < 10; i++) {
    u32 s = (u32)((code >> (6 * (9 - i))) & 63);
    if (!s) break;
    out.push_back(UNSYM[s]);
  }
}
// decode without lock (only call when no concurrent interning)
static inline u32 decode_fast(u64 code, char* out) {
  if (code & LONGBIT) {
    const std::string& s = g_istr[(u32)(code & 0xffffffffu)];
    memcpy(out, s.data(), s.size());
    return (u32)s.size();
  }
  u32 n = 0;
  for (int i = 0; i < 10; i++) {
    u32 s = (u32)((code >> (6 * (9 - i))) & 63);
    if (!s) break;
    out[n++] = UNSYM[s];
  }
  return n;
}
static inline u32 code_len_bound(u64 code) {
  if (code & LONGBIT) return (u32)g_istr[(u32)(code & 0xffffffffu)].size();
  return 10;
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
static inline int dim(i64 y, int m) {
  static const int D[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  return m == 2 ? (is_leap(y) ? 29 : 28) : D[m];
}
static inline bool d2(const char* p, int& v) {
  unsigned a = (unsigned)(uint8_t)p[0] - '0', b = (unsigned)(uint8_t)p[1] - '0';
  if (a > 9 || b > 9) return false;
  v = (int)(a * 10 + b);
  return true;
}
// Returns false for a bad timestamp; values of F3 that are absurdly large are also "bad"
// since they can't be inside any window.
static bool parse_ts(const char* p, u32 n, i64& out) {
  if (n == 0) return false;
  if ((n == 20 || n == 25) && p[4] == '-') {
    int y1, y2, M, D, h, mi, s;
    if (!d2(p, y1) || !d2(p + 2, y2) || !d2(p + 5, M) || p[7] != '-' || !d2(p + 8, D) || p[10] != 'T' ||
        !d2(p + 11, h) || p[13] != ':' || !d2(p + 14, mi) || p[16] != ':' || !d2(p + 17, s))
      return false;
    i64 Y = y1 * 100 + y2;
    if (M < 1 || M > 12 || D < 1 || D > dim(Y, M) || h > 23 || mi > 59 || s > 59) return false;
    i64 off = 0;
    if (n == 20) {
      if (p[19] != 'Z') return false;
    } else {
      char sg = p[19];
      int oh, om;
      if ((sg != '+' && sg != '-') || !d2(p + 20, oh) || p[22] != ':' || !d2(p + 23, om)) return false;
      if (oh > 23 || om > 59) return false;
      off = (i64)(oh * 60 + om) * 60000;
      if (sg == '-') off = -off;
    }
    out = (days_from_civil(Y, M, D) * 86400 + h * 3600 + mi * 60 + s) * 1000 - off;
    return true;
  }
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  u32 sig = n - i;
  i64 v = 0;
  for (u32 j = i; j < n; j++) {
    unsigned d = (unsigned)(uint8_t)p[j] - '0';
    if (d > 9) return false;
    if (sig <= 17) v = v * 10 + d;
  }
  if (sig > 17) return false;  // > 1e17 ms: far outside any window
  out = v;
  return true;
}

static bool parse_price(const char* p, u32 n, u32& out) {
  if (n == 0) return false;
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  if (n - i > 8) {
    for (u32 j = i; j < n; j++)
      if ((unsigned)(uint8_t)p[j] - '0' > 9) return false;
    return false;
  }
  u32 v = 0;
  for (u32 j = i; j < n; j++) {
    unsigned d = (unsigned)(uint8_t)p[j] - '0';
    if (d > 9) return false;
    v = v * 10 + d;
  }
  if (v < 1 || v > 10000000) return false;
  out = v;
  return true;
}

// ---------------------------------------------------------------- CSV tokenizer
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

static inline int ctz128(u128 x) {
  u64 lo = (u64)x;
  if (lo) return __builtin_ctzll(lo);
  return 64 + __builtin_ctzll((u64)(x >> 64));
}
static inline int pop128(u128 x) { return __builtin_popcountll((u64)x) + __builtin_popcountll((u64)(x >> 64)); }

// Generic scalar tokenizer for one line [p, nl). Returns false if malformed.
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

// Tokenize the line starting at p. Sets next to the start of the following line.
template <int N>
static inline bool tok(const char* p, const char* fe, const char*& next, Fld* f) {
  if (fe - p >= 128) {
    u64 c0, q0, n0;
    masks64(p, c0, q0, n0);
    u128 C, Q;
    u32 L;
    if (n0) {
      L = (u32)__builtin_ctzll(n0);
      u64 lm = L ? ((~0ULL) >> (64 - L)) : 0;
      C = c0 & lm;
      Q = q0 & lm;
    } else {
      u64 c1, q1, n1;
      masks64(p + 64, c1, q1, n1);
      if (!n1) goto slow;
      L = 64 + (u32)__builtin_ctzll(n1);
      u64 lm = (L > 64) ? ((~0ULL) >> (128 - L)) : 0;
      C = (u128)c0 | ((u128)(c1 & lm) << 64);
      Q = (u128)q0 | ((u128)(q1 & lm) << 64);
    }
    next = p + L + 1;
    if (pop128(C) != N - 1) return false;
    u32 s = 0;
    for (int i = 0; i < N - 1; i++) {
      u32 e = (u32)ctz128(C);
      C &= C - 1;
      f[i].p = p + s;
      f[i].n = e - s;
      s = e + 1;
    }
    f[N - 1].p = p + s;
    f[N - 1].n = L - s;
    if (Q) {
      for (int i = 0; i < N; i++) {
        u32 st = (u32)(f[i].p - p), len = f[i].n;
        u128 lowm = len >= 128 ? ~(u128)0 : (((u128)1 << len) - 1);
        u128 bits = (Q >> st) & lowm;
        if (!bits) continue;
        if (len >= 2 && bits == ((u128)1 | ((u128)1 << (len - 1)))) {
          f[i].p++;
          f[i].n -= 2;
        } else
          return false;
      }
    }
    return true;
  }
slow:
  const char* e = (const char*)memchr(p, '\n', (size_t)(fe - p));
  if (!e) {
    e = fe;
    next = fe;
  } else
    next = e + 1;
  return tok_scalar<N>(p, e, f);
}

// ---------------------------------------------------------------- small hash tables
static inline u64 hmix(u64 x, u64 k) { return (x ^ (x >> 31)) * k; }
static const u64 K1 = 0x9E3779B97F4A7C15ULL, K2 = 0xC2B2AE3D27D4EB4FULL, K3 = 0x165667B19E3779F9ULL,
                 K4 = 0xD6E8FEB86659FD93ULL, K5 = 0xFF51AFD7ED558CCDULL;

struct SmallTab {  // u64 -> u32, single threaded build
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

// Concurrent link table: code -> slot, plus union-find
struct LinkTab {
  std::atomic<u64>* keys = nullptr;
  u64* name = nullptr;
  u32* parent = nullptr;
  u64 mask = 0;
  int shift = 64;
  void init(size_t n) {
    size_t cap = 1024;
    int lg = 10;
    while (cap < n * 2) { cap <<= 1; lg++; }
    keys = (std::atomic<u64>*)calloc(cap, sizeof(u64));
    name = (u64*)malloc(cap * sizeof(u64));
    parent = (u32*)malloc(cap * sizeof(u32));
    mask = cap - 1;
    shift = 64 - lg;
  }
  inline u32 insert(u64 k, std::vector<u32>& fresh) {
    u64 i = hmix(k, K4) >> shift;
    while (true) {
      u64 x = keys[i].load(std::memory_order_relaxed);
      if (x == k) return (u32)i;
      if (x == 0) {
        u64 exp = 0;
        if (keys[i].compare_exchange_strong(exp, k, std::memory_order_relaxed)) {
          name[i] = k;
          parent[i] = (u32)i;
          fresh.push_back((u32)i);
          return (u32)i;
        }
        if (exp == k) return (u32)i;
      }
      i = (i + 1) & mask;
    }
  }
  inline u64 slot_of(u64 k) const { return hmix(k, K4) >> shift; }
  inline void prefetch(u64 k) const { __builtin_prefetch((const void*)&keys[slot_of(k)]); }
  inline u64 lookup(u64 k) const {
    u64 i = slot_of(k);
    const u64* kk = (const u64*)keys;
    while (true) {
      u64 x = kk[i];
      if (x == k) return name[i];
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
struct Rec1 {  // event copy, partitioned by event_id
  u64 eid;
  u64 user;
  u32 ts;    // ms since window_start
  i32 ing;   // ingest seconds since window_start (clamped)
  u32 pc;    // price | cat << 24 (purchase only), 0 for view/cart
  u32 valid;
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

// ---------------------------------------------------------------- globals
static int NT = 8;
static i64 WS, WE, CB;  // window start/end, click base
static i64 DAY0;
static int NDAYS;
static int P1BITS, P2BITS, P1, P2;
static SmallTab g_prod;   // product code -> category index
static SmallTab g_camp;   // campaign code -> 1-based index
static std::vector<std::string> g_cats;  // sorted category names
static u32 g_unknown_cat;
static std::vector<std::string> g_camp_id, g_camp_ch;  // index 0 = organic
static LinkTab g_links;
static int g_nolookup = 0;

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
  u32 topcat;  // UINT32_MAX = null
  i64 sessions, events, rev;
};

struct alignas(64) TL {
  std::vector<std::vector<Rec1>> b1;
  std::vector<ClickRec> clicks;
  std::vector<std::pair<u32, u32>> links;
  std::vector<u32> fresh;
  std::vector<std::vector<Rec2>> b2;
  std::vector<std::vector<ClickRec>> cb;
  std::vector<UserRow> rows;
  std::vector<CD> cd;
  size_t ev_lines = 0;
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
  void* m = mmap(nullptr, n, PROT_READ, MAP_PRIVATE, fd, 0);
  close(fd);
  if (m == MAP_FAILED) return;
  g_files.push_back({(const char*)m, n, kind});
}

// iterate the data lines of a small in-memory CSV (skips header)
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

static void parse_events(TL& tl, const char* p, const char* end, const char* fe) {
  Fld f[7];
  const i64 wlen = WE - WS;
  size_t lines = 0;
  while (p < end) {
    const char* next;
    bool ok = tok<7>(p, fe, next, f);
    p = next;
    lines++;
    if (!ok || f[0].n == 0) continue;
    Rec1 r;
    r.eid = encode(f[0].p, f[0].n);
    i64 ing;
    if (!parse_ts(f[3].p, f[3].n, ing)) {
      ing = INT64_MAX / 4;
    }
    i64 ings = (ing - WS) / 1000;  // ingest is whole seconds (F1)
    if (ing < WS && (ing - WS) % 1000) ings--;
    if (ings > INT32_MAX) ings = INT32_MAX;
    if (ings < INT32_MIN) ings = INT32_MIN;
    r.ing = (i32)ings;
    r.valid = 0;
    r.user = 0;
    r.ts = 0;
    r.pc = 0;
    do {
      if (f[1].n == 0 || f[5].n == 0) break;
      i64 ts;
      if (!parse_ts(f[2].p, f[2].n, ts)) break;
      ts -= WS;
      if (ts < 0 || ts >= wlen) break;
      if (ing - (ts + WS) > DAYMS) break;
      u32 pc = 0;
      const char* t = f[4].p;
      if (f[4].n == 4) {
        if (!(memcmp(t, "view", 4) == 0 || memcmp(t, "cart", 4) == 0)) break;
      } else if (f[4].n == 8 && memcmp(t, "purchase", 8) == 0) {
        u32 price;
        if (!parse_price(f[6].p, f[6].n, price)) break;
        u32 cat;
        u64 pcode = encode(f[5].p, f[5].n);
        if (!g_prod.get(pcode, cat)) cat = g_unknown_cat;
        pc = price | (cat << 24);
      } else
        break;
      r.user = encode(f[1].p, f[1].n);
      r.ts = (u32)ts;
      r.pc = pc;
      r.valid = 1;
    } while (0);
    tl.b1[p1_of(r.eid)].push_back(r);
  }
  tl.ev_lines += lines;
}

static void parse_clicks(TL& tl, const char* p, const char* end, const char* fe) {
  Fld f[4];
  const i64 clen = WE - CB;
  while (p < end) {
    const char* next;
    bool ok = tok<4>(p, fe, next, f);
    p = next;
    if (!ok || f[0].n == 0 || f[1].n == 0 || f[2].n == 0) continue;
    i64 ts;
    if (!parse_ts(f[3].p, f[3].n, ts)) continue;
    ts -= CB;
    if (ts < 0 || ts >= clen) continue;
    u32 camp;
    if (!g_camp.get(encode(f[2].p, f[2].n), camp)) continue;
    ClickRec c;
    c.who = encode(f[1].p, f[1].n);
    c.cid = encode(f[0].p, f[0].n);
    c.ts = (u32)ts;
    c.camp = camp;
    tl.clicks.push_back(c);
  }
}

static void parse_links(TL& tl, const char* p, const char* end, const char* fe) {
  Fld f[2];
  while (p < end) {
    const char* next;
    bool ok = tok<2>(p, fe, next, f);
    p = next;
    if (!ok || f[0].n == 0 || f[1].n == 0) continue;
    u32 a = g_links.insert(encode(f[0].p, f[0].n), tl.fresh);
    u32 b = g_links.insert(encode(f[1].p, f[1].n), tl.fresh);
    if (a != b) tl.links.push_back({a, b});
  }
}

static void run_union_find() {
  LinkTab& L = g_links;
  for (auto& tl : g_tl) {
    for (auto& pr : tl.links) {
      u32 a = L.find(pr.first), b = L.find(pr.second);
      if (a == b) continue;
      // root keeps the smaller name
      if (key_less(L.name[b], L.name[a])) std::swap(a, b);
      L.parent[b] = a;
    }
  }
  for (auto& tl : g_tl)
    for (u32 s : tl.fresh) L.name[s] = L.name[L.find(s)];
}

// ---------------------------------------------------------------- phase 2: dedup + person mapping
struct DedupScratch {
  std::vector<u64> keys;
  std::vector<Rec1*> ptr;
  std::vector<Rec1*> win;
};

static void dedup_partition(int tid, int p, DedupScratch& S) {
  TL& me = g_tl[tid];
  size_t n = 0;
  for (auto& tl : g_tl) n += tl.b1[p].size();
  if (!n) return;
  size_t cap = 16;
  int lg = 4;
  while (cap < n * 2) { cap <<= 1; lg++; }
  if (S.keys.size() < cap) { S.keys.resize(cap); S.ptr.resize(cap); }
  memset(S.keys.data(), 0, cap * sizeof(u64));
  u64 mask = cap - 1;
  int sh = 64 - lg;
  size_t used = 0;
  for (auto& tl : g_tl) {
    for (Rec1& r : tl.b1[p]) {
      u64 i = hmix(r.eid, K3) >> sh;
      while (true) {
        u64 k = S.keys[i];
        if (k == r.eid) {
          if (r.ing < S.ptr[i]->ing) S.ptr[i] = &r;
          break;
        }
        if (!k) {
          S.keys[i] = r.eid;
          S.ptr[i] = &r;
          used++;
          break;
        }
        i = (i + 1) & mask;
      }
    }
  }
  S.win.clear();
  if (used == n) {
    for (auto& tl : g_tl)
      for (Rec1& r : tl.b1[p])
        if (r.valid) S.win.push_back(&r);
  } else {
    for (size_t i = 0; i < cap; i++)
      if (S.keys[i] && S.ptr[i]->valid) S.win.push_back(S.ptr[i]);
  }
  const size_t D = 16;
  size_t m = S.win.size();
  for (size_t i = 0; i < m && i < D; i++) g_links.prefetch(S.win[i]->user);
  for (size_t i = 0; i < m; i++) {
    if (i + D < m) g_links.prefetch(S.win[i + D]->user);
    Rec1* r = S.win[i];
    u64 person = g_nolookup ? r->user : g_links.lookup(r->user);
    me.b2[p2_of(person)].push_back({person, r->ts, r->pc});
  }
}

static void map_clicks(int tid, std::vector<ClickRec>& v, size_t beg, size_t end) {
  TL& me = g_tl[tid];
  const size_t D = 16;
  for (size_t i = beg; i < end && i < beg + D; i++) g_links.prefetch(v[i].who);
  for (size_t i = beg; i < end; i++) {
    if (i + D < end) g_links.prefetch(v[i + D].who);
    ClickRec c = v[i];
    c.who = g_links.lookup(c.who);
    me.cb[p2_of(c.who)].push_back(c);
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
  std::vector<u32> tab;  // index+1 into ents
  std::vector<PEnt> ents;
  std::vector<u32> eidx;
  std::vector<u64> ev;
  std::vector<u32> cidx;
  std::vector<ClickE> cl;
  std::vector<u32> ccnt, cpos, clist;
  std::vector<i64> catsum;
  std::vector<u32> cattouch;
};

static void radix_sort_u64(u64* a, size_t n, std::vector<u64>& tmp) {
  // sort by low 32 bits..? we sort whole key (ts<<32|pc) by the top 32 bits (ts) using 3 passes of 11 bits
  tmp.resize(n);
  u64* src = a;
  u64* dst = tmp.data();
  for (int pass = 0; pass < 3; pass++) {
    int sh = 32 + pass * 11;
    size_t cnt[2048] = {0};
    for (size_t i = 0; i < n; i++) cnt[(src[i] >> sh) & 2047]++;
    size_t s = 0;
    for (int b = 0; b < 2048; b++) { size_t c = cnt[b]; cnt[b] = s; s += c; }
    for (size_t i = 0; i < n; i++) dst[cnt[(src[i] >> sh) & 2047]++] = src[i];
    std::swap(src, dst);
  }
  if (src != a) memcpy(a, src, n * sizeof(u64));
}

static inline bool click_less(const ClickE& a, const ClickE& b) {
  if (a.ts != b.ts) return a.ts < b.ts;
  return key_less(a.cid, b.cid);
}

static void session_partition(int tid, int q, SessScratch& S) {
  TL& me = g_tl[tid];
  size_t n = 0;
  for (auto& tl : g_tl) n += tl.b2[q].size();
  if (!n) return;
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
  size_t k = 0;
  for (auto& tl : g_tl) {
    for (const Rec2& r : tl.b2[q]) {
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
      S.eidx[k++] = e;
    }
  }
  u32 off = 0;
  for (auto& e : S.ents) { e.off = off; off += e.cnt; e.cnt = 0; }
  S.ev.resize(n);
  k = 0;
  for (auto& tl : g_tl)
    for (const Rec2& r : tl.b2[q]) {
      PEnt& e = S.ents[S.eidx[k++]];
      S.ev[e.off + e.cnt++] = ((u64)r.ts << 32) | r.pc;
    }
  // clicks
  size_t nc = 0;
  for (auto& tl : g_tl) nc += tl.cb[q].size();
  S.cidx.resize(nc);
  k = 0;
  for (auto& tl : g_tl)
    for (const ClickRec& c : tl.cb[q]) {
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
    }
  off = 0;
  for (auto& e : S.ents) { e.coff = off; off += e.ccnt; e.ccnt = 0; }
  S.cl.resize(off);
  k = 0;
  for (auto& tl : g_tl)
    for (const ClickRec& c : tl.cb[q]) {
      u32 ei = S.cidx[k++];
      if (ei == UINT32_MAX) continue;
      PEnt& e = S.ents[ei];
      S.cl[e.coff + e.ccnt++] = {c.ts, c.camp, c.cid};
    }

  std::vector<u64> tmp;
  const u32 ncamp = (u32)g_camp_id.size();
  if (S.ccnt.size() < ncamp) { S.ccnt.assign(ncamp, 0); S.cpos.assign(ncamp, 0); }
  if (S.catsum.size() < g_cats.size()) S.catsum.assign(g_cats.size(), 0);
  CD* cd = me.cd.data();
  const int ND = NDAYS;

  for (auto& e : S.ents) {
    u64* ev = S.ev.data() + e.off;
    u32 m = e.cnt;
    if (m > 1) {
      if (m < 24) {
        for (u32 i = 1; i < m; i++) {
          u64 x = ev[i];
          u32 j = i;
          while (j > 0 && ev[j - 1] > x) { ev[j] = ev[j - 1]; j--; }
          ev[j] = x;
        }
      } else if (m < 50000)
        std::sort(ev, ev + m);
      else
        radix_sort_u64(ev, m, tmp);
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
    // walk sessions
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
        if (v > best || (v == best && c < top)) { best = v; top = c; }
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
      i32 day = (i32)(((WS + (i64)s) - (DAY0 * DAYMS)) / DAYMS);
      if (day != cur_day) {
        flush_day();
        cur_day = day;
      }
      u32 j = i;
      while (j < m) {
        u32 t = (u32)(ev[j] >> 32);
        if ((i64)t - (i64)prev > GAPMS) break;
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
      // attribution: clicks with ts in [s, s + WEEK] in click base
      u64 sc = (u64)s + (u64)WEEKMS;
      while (hi < kc && cl[hi].ts <= sc) {
        u32 c = cl[hi].camp;
        if (S.ccnt[c]++ == 0) { S.cpos[c] = (u32)S.clist.size(); S.clist.push_back(c); }
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
  // user_id
  {
    std::string& s = cols[0].values;
    s.reserve(n * 14);
    char buf[64];
    for (auto& r : rows) {
      if (r.name & LONGBIT) {
        const std::string& x = g_istr[(u32)(r.name & 0xffffffffu)];
        pq::put_str(s, x.data(), (u32)x.size());
      } else {
        u32 len = decode_fast(r.name, buf);
        pq::put_str(s, buf, len);
      }
    }
  }
  {
    std::string& s = cols[1].values;
    s.resize(n * 4);
    char* o = &s[0];
    for (auto& r : rows) { memcpy(o, &r.day, 4); o += 4; }
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
      if (r.topcat == UINT32_MAX) dl.add(0);
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
  init_sym();
  const char* e;
  std::string DATA = (e = getenv("DATA_DIR")) ? e : "/data";
  std::string OUT = (e = getenv("OUTPUT_DIR")) ? e : "/output";
  g_nolookup = getenv("NOLOOKUP") ? 1 : 0;
  NT = (e = getenv("NCPU")) ? atoi(e) : (int)std::thread::hardware_concurrency();
  if (NT < 1) NT = 1;
  if (NT > 64) NT = 64;

  // manifest
  {
    std::string m = read_file(DATA + "/manifest.json");
    std::string a, b;
    if (!find_json_str(m, "window_start", a) || !find_json_str(m, "window_end", b) ||
        !parse_ts(a.data(), (u32)a.size(), WS) || !parse_ts(b.data(), (u32)b.size(), WE)) {
      fprintf(stderr, "bad manifest\n");
      return 1;
    }
    CB = WS - WEEKMS;
    if (WE - CB >= (i64)UINT32_MAX || WE <= WS) {
      fprintf(stderr, "window too long\n");
      return 1;
    }
    DAY0 = floordiv(WS, DAYMS);
    NDAYS = (int)(floordiv(WE - 1, DAYMS) - DAY0 + 1);
  }
  // products & categories
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
    for (auto& pr : prods) g_prod.put(encode(pr.first.data(), (u32)pr.first.size()), ci[pr.second]);
  }
  // campaigns
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
      u64 code = encode(c.first.data(), (u32)c.first.size());
      if (g_camp.get(code, tmp)) continue;
      g_camp.put(code, (u32)g_camp_id.size());
      g_camp_id.push_back(c.first);
      g_camp_ch.push_back(c.second);
    }
  }
  // files
  size_t ev_bytes = 0, link_bytes = 0;
  for (auto& f : list_csv(DATA + "/links")) map_file(f, 2);
  for (auto& f : list_csv(DATA + "/events")) map_file(f, 0);
  for (auto& f : list_csv(DATA + "/clicks")) map_file(f, 1);
  for (auto& f : g_files) {
    if (f.kind == 0) ev_bytes += f.size;
    if (f.kind == 2) link_bytes += f.size;
  }
  size_t ev_est = ev_bytes / 70 + 1024;
  P1BITS = 4;
  while ((1u << P1BITS) * 24000ull < ev_est && P1BITS < 11) P1BITS++;
  P2BITS = P1BITS;
  P1 = 1 << P1BITS;
  P2 = 1 << P2BITS;
  g_links.init(link_bytes / 10 + 1024);
  logt("setup");

  // chunks: links first, then the rest (largest first)
  std::vector<Chunk> chunks;
  const size_t CH = 4 << 20;
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
  size_t per = ev_est / ((size_t)NT * P1) + 16;
  std::atomic<size_t> next_chunk{0};
  std::atomic<int> links_left{nlinkchunks};
  std::atomic<bool> uf_done{nlinkchunks == 0};
  if (nlinkchunks == 0) run_union_find();
  parallel(NT, [&](int tid) {
    TL& tl = g_tl[tid];
    tl.b1.resize(P1);
    for (auto& v : tl.b1) v.reserve(per + per / 4);
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
      const char* end = base + c.end;
      if (fm.kind == 0) parse_events(tl, p, end, fe);
      else if (fm.kind == 1) parse_clicks(tl, p, end, fe);
      else {
        parse_links(tl, p, end, fe);
        if (links_left.fetch_sub(1) == 1) {
          run_union_find();
          uf_done.store(true);
        }
      }
    }
  });
  logt("parse");

  // phase 2
  {
    struct Item { int kind; int a; size_t b, c; };
    std::vector<Item> items;
    for (int t = 0; t < NT; t++) {
      size_t n = g_tl[t].clicks.size();
      const size_t S = 1 << 16;
      for (size_t b = 0; b < n; b += S) items.push_back({1, t, b, std::min(n, b + S)});
    }
    for (int p = 0; p < P1; p++) items.push_back({0, p, 0, 0});
    std::atomic<size_t> nx{0};
    parallel(NT, [&](int tid) {
      TL& me = g_tl[tid];
      me.b2.resize(P2);
      me.cb.resize(P2);
      size_t per2 = ev_est / ((size_t)NT * P2) + 16;
      for (auto& v : me.b2) v.reserve(per2);
      DedupScratch S;
      while (true) {
        size_t i = nx.fetch_add(1);
        if (i >= items.size()) break;
        Item it = items[i];
        if (it.kind == 0) dedup_partition(tid, it.a, S);
        else map_clicks(tid, g_tl[it.a].clicks, it.b, it.c);
      }
    });
  }
  logt("dedup");
  // free phase-1 buffers in parallel
  parallel(NT, [&](int tid) {
    std::vector<std::vector<Rec1>>().swap(g_tl[tid].b1);
    std::vector<ClickRec>().swap(g_tl[tid].clicks);
  });

  // phase 3
  {
    std::vector<std::pair<size_t, int>> order;
    for (int q = 0; q < P2; q++) {
      size_t n = 0;
      for (auto& tl : g_tl) n += tl.b2[q].size();
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

  // output
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
    if (tid == 0) {
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
