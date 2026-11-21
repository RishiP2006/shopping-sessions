// Differential checks for the SIMD scanner against the scalar CSV tokenizer.
// Build: c++ -O2 -std=c++17 -pthread tools/check_scanner.cpp -o /tmp/check-scanner
#define main pipeline_entry_for_test
#include "../src/pipeline.cpp"
#undef main
#include <cassert>

static u64 rng_state = 0x947ccb03a67e1f29ULL;
static u32 rnd(u32 n) {
  rng_state ^= rng_state << 13;
  rng_state ^= rng_state >> 7;
  rng_state ^= rng_state << 17;
  return (u32)(rng_state % n);
}
using Rows = std::vector<std::vector<std::string>>;

template<int N>
static void check(const std::string& input, size_t cut, bool eof) {
  std::string padded = input + std::string(512, '\0');
  const char* begin = padded.data();
  const char* end = begin + input.size();
  const char* limit = begin + cut;
  const char* expected_end = begin;
  Rows expected, actual;
  auto collect = [](Rows& rows, Fld* f) {
    std::vector<std::string> row;
    for (int i = 0; i < N; ++i) row.emplace_back(f[i].p, f[i].n);
    rows.push_back(std::move(row));
  };
  while (expected_end < end && expected_end < limit) {
    const char* nl = (const char*)memchr(expected_end, '\n', (size_t)(end - expected_end));
    if (!nl && !eof) break;
    Fld f[N];
    if (tok_scalar<N>(expected_end, nl ? nl : end, f)) collect(expected, f);
    expected_end = nl ? nl + 1 : end;
  }
  const char* actual_end = scan<N>(begin, limit, end, eof, [&](Fld* f) { collect(actual, f); });
  if (actual != expected || actual_end != expected_end) {
    fprintf(stderr, "scanner mismatch: N=%d length=%zu cut=%zu eof=%d expected-end=%td actual-end=%td\n",
            N, input.size(), cut, eof, expected_end - begin, actual_end - begin);
    for (unsigned char c : input) fprintf(stderr, "%02x", (unsigned)c);
    fprintf(stderr, "\nexpected=%zu actual=%zu\n", expected.size(), actual.size());
    for (const auto& row : expected) { fprintf(stderr, "E"); for (const auto& f : row) fprintf(stderr, " %zu", f.size()); fprintf(stderr, "\n"); }
    for (const auto& row : actual) { fprintf(stderr, "A"); for (const auto& f : row) fprintf(stderr, " %zu", f.size()); fprintf(stderr, "\n"); }
    Rows old;
    scan_old<N>(begin, limit, end, eof, [&](Fld* f) { collect(old, f); });
    fprintf(stderr, "old matches expected: %d\n", old == expected);
    abort();
  }
}

template<int N>
static void run() {
  const char alphabet[] = "abc019 _\r\t\xc3\xa9";
  for (int trial = 0; trial < 20000; ++trial) {
    std::string input;
    u32 count = 1 + rnd(12);
    for (u32 row = 0; row < count; ++row) {
      int fields = rnd(10) ? N : (int)rnd(N + 3);
      for (int f = 0; f < fields; ++f) {
        if (f) input += ',';
        std::string value;
        u32 len = rnd(5) ? rnd(12) : rnd(170);
        for (u32 k = 0; k < len; ++k) value += alphabet[rnd(sizeof(alphabet) - 1)];
        u32 mode = rnd(10);
        if (mode < 3) value = '"' + value + '"';
        if (mode == 3) value.insert(rnd((u32)value.size() + 1), 1, '"');
        if (mode == 4) value += '\0';
        input += value;
      }
      if (row + 1 < count || rnd(2)) input += '\n';
    }
    check<N>(input, input.size(), true);
    check<N>(input, rnd((u32)input.size() + 1), false);
    // Force read boundaries around SIMD block sizes and within quoted fields.
    size_t tail = std::min(input.size(), (size_t)(60 + rnd(73)));
    check<N>(input.substr(0, tail), tail, false);
  }
}
int main() {
  // Commas always delimit fields, including inside incorrectly placed quotes.
  std::string bad = "\"" + std::string(200, 'a') + ",b\",c";
  Fld fields[2];
  assert(!tok_scalar<2>(bad.data(), bad.data() + bad.size(), fields));
  check<2>(bad, bad.size(), true);
  run<2>(); run<4>(); run<7>();
  puts("180000 scanner comparisons and regression checks passed");
}
