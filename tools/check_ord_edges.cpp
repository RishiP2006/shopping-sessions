// Edge cases for the delimiter-ordinal scanner vs tok_scalar (build with -DORD_SCAN).
#define main pipeline_entry_for_test
#include "../src/pipeline.cpp"
#undef main
template <int N>
static int run(const std::string& in, bool eof) {
  std::string pad = in + std::string(512, '\0');
  const char* b = pad.data();
  const char* e = b + in.size();
  std::vector<std::vector<std::string>> exp, act;
  const char* x = b;
  while (x < e) {
    const char* nl = (const char*)memchr(x, '\n', (size_t)(e - x));
    if (!nl && !eof) break;
    Fld f[N];
    if (tok_scalar<N>(x, nl ? nl : e, f)) {
      std::vector<std::string> r;
      for (int i = 0; i < N; i++) r.emplace_back(f[i].p, f[i].n);
      exp.push_back(r);
    }
    x = nl ? nl + 1 : e;
  }
  const char* r = scan<N>(b, e + 1, e, eof, [&](Fld* f) {
    std::vector<std::string> v;
    for (int i = 0; i < N; i++) v.emplace_back(f[i].p, f[i].n);
    act.push_back(v);
  });
  if (exp != act || r != x) {
    fprintf(stderr, "MISMATCH N=%d len=%zu eof=%d rows %zu vs %zu end %td vs %td\n", N, in.size(), eof, exp.size(),
            act.size(), x - b, r - b);
    return 1;
  }
  return 0;
}
int main() {
  int bad = 0;
  std::string ok7 = "e1,u1,1791071890000,2026-10-03T10:00:00Z,view,p1,\n";
  std::string q7 = "\"e1\",\"u1\",\"1791071890000\",2026-10-03T10:00:00Z,\"view\",p1,\"\"\n";
  // 1. thousands of commas in one line, then normal lines
  for (int k : {100, 2100, 5000, 9000}) {
    std::string s = ok7 + std::string(k, ',') + "\n" + ok7 + q7;
    for (bool eof : {true, false}) bad += run<7>(s, eof);
  }
  // 2. a quote / comma / newline at every offset around 64-byte boundaries
  for (int pre = 0; pre < 200; pre++) {
    std::string lead(pre, 'x');
    for (const char* mid : {"\"a\"", "\"", "a\"", "\"a", "\"\"", "\"\"\"", ",", "a,\"b\",c", "\"\n\""}) {
      std::string s = "e" + lead + "," + mid + ",x,y,z,w,v\n" + ok7 + q7 + lead + "\n" + q7;
      for (bool eof : {true, false}) bad += run<7>(s, eof);
      std::string c = lead + "," + mid + ",z,w\n" + lead + ",\"b\",c,d\n";
      for (bool eof : {true, false}) bad += run<4>(c, eof);
    }
  }
  // 3. bursts of empty lines, final line without newline at exact block multiples
  for (int n = 0; n < 2000; n += 37) {
    std::string s = std::string(n, '\n') + ok7 + std::string(n, '\n') + "a,b";
    for (bool eof : {true, false}) bad += run<2>(s, eof);
  }
  for (int len = 60; len < 300; len++) {
    std::string s = ok7;
    while (s.size() + 1 < (size_t)len) s += 'q';
    s = s.substr(0, (size_t)len);
    std::string t = ok7 + "e2,u2,3,4,view,p1,\"" + std::string(len % 70, 'z') + "\"";
    for (bool eof : {true, false}) {
      bad += run<7>(s, eof);
      bad += run<7>(t, eof);
    }
  }
  if (bad) {
    fprintf(stderr, "%d edge mismatches\n", bad);
    return 1;
  }
  printf("ordinal scanner edge checks passed\n");
  return 0;
}
