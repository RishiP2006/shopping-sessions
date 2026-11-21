// Minimal Parquet writer: one row group, one PLAIN data page per column, no compression.
#pragma once
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace pq {

enum PType { BOOLEAN = 0, INT32 = 1, INT64 = 2, BYTE_ARRAY = 6 };
enum Logical { L_NONE = 0, L_STRING = 1, L_DATE = 6 };

struct Thrift {
  std::string b;
  std::vector<int> stack;
  int last = 0;
  void byte(uint8_t x) { b.push_back((char)x); }
  void varint(uint64_t v) {
    while (v >= 0x80) { byte((uint8_t)(v | 0x80)); v >>= 7; }
    byte((uint8_t)v);
  }
  void zz32(int32_t v) { varint((uint32_t)(((uint32_t)v << 1) ^ (uint32_t)(v >> 31))); }
  void zz64(int64_t v) { varint((uint64_t)(((uint64_t)v << 1) ^ (uint64_t)(v >> 63))); }
  void fh(int id, int type) {
    int d = id - last;
    if (d > 0 && d <= 15) byte((uint8_t)((d << 4) | type));
    else { byte((uint8_t)type); zz32(id); }
    last = id;
  }
  void i32(int id, int32_t v) { fh(id, 5); zz32(v); }
  void i64(int id, int64_t v) { fh(id, 6); zz64(v); }
  void bin(int id, const std::string& s) { fh(id, 8); varint(s.size()); b += s; }
  void sbegin(int id) { fh(id, 12); stack.push_back(last); last = 0; }
  void send() { byte(0); last = stack.back(); stack.pop_back(); }
  void lbegin(int id, int etype, int n) {
    fh(id, 9);
    if (n < 15) byte((uint8_t)((n << 4) | etype));
    else { byte((uint8_t)(0xF0 | etype)); varint(n); }
  }
  void ebegin() { stack.push_back(last); last = 0; }  // struct element inside a list
  void eend() { byte(0); last = stack.back(); stack.pop_back(); }
  void lbin(const std::string& s) { varint(s.size()); b += s; }
  void li32(int32_t v) { zz32(v); }
};

struct Column {
  std::string name;
  int ptype;
  int logical;
  bool optional;
  std::string values;  // PLAIN-encoded non-null values
  std::string deflev;  // for optional: 4-byte length prefix + RLE hybrid data
};

// RLE-hybrid encoding of 0/1 definition levels (bit width 1), with length prefix.
struct DefLevels {
  std::string out;
  uint8_t cur = 2;
  uint64_t run = 0;
  void flush() {
    if (run == 0) return;
    uint64_t v = run << 1;
    while (v >= 0x80) { out.push_back((char)(v | 0x80)); v >>= 7; }
    out.push_back((char)v);
    out.push_back((char)cur);
    run = 0;
  }
  void add(uint8_t v) {
    if (v == cur) { run++; return; }
    flush();
    cur = v;
    run = 1;
  }
  std::string finish() {
    flush();
    std::string r(4, '\0');
    uint32_t n = (uint32_t)out.size();
    memcpy(&r[0], &n, 4);
    r += out;
    return r;
  }
};

inline bool write_file(const std::string& path, int64_t nrows, std::vector<Column>& cols) {
  FILE* f = fopen(path.c_str(), "wb");
  if (!f) return false;
  std::string head = "PAR1";
  fwrite(head.data(), 1, 4, f);
  int64_t off = 4;
  struct Meta { int64_t page_off, total; };
  std::vector<Meta> metas;
  for (auto& c : cols) {
    if (nrows == 0) break;
    Thrift t;
    int64_t body = (int64_t)c.values.size() + (int64_t)(c.optional ? c.deflev.size() : 0);
    t.i32(1, 0);              // DATA_PAGE
    t.i32(2, (int32_t)body);  // uncompressed
    t.i32(3, (int32_t)body);  // compressed
    t.sbegin(5);
    t.i32(1, (int32_t)nrows);
    t.i32(2, 0);  // PLAIN
    t.i32(3, 3);  // RLE
    t.i32(4, 3);  // RLE
    t.send();
    t.byte(0);
    fwrite(t.b.data(), 1, t.b.size(), f);
    if (c.optional) fwrite(c.deflev.data(), 1, c.deflev.size(), f);
    fwrite(c.values.data(), 1, c.values.size(), f);
    int64_t total = (int64_t)t.b.size() + body;
    metas.push_back({off, total});
    off += total;
    std::string().swap(c.values);
    std::string().swap(c.deflev);
  }
  Thrift m;
  m.i32(1, 1);
  m.lbegin(2, 12, (int)cols.size() + 1);
  m.ebegin();
  m.bin(4, "schema");
  m.i32(5, (int32_t)cols.size());
  m.eend();
  for (auto& c : cols) {
    m.ebegin();
    m.i32(1, c.ptype);
    m.i32(3, c.optional ? 1 : 0);
    m.bin(4, c.name);
    if (c.logical == L_STRING) m.i32(6, 0);
    if (c.logical == L_DATE) m.i32(6, 6);
    if (c.logical != L_NONE) {
      m.sbegin(10);
      m.sbegin(c.logical);
      m.send();
      m.send();
    }
    m.eend();
  }
  m.i64(3, nrows);
  int64_t total_bytes = 0;
  for (auto& x : metas) total_bytes += x.total;
  if (nrows > 0) {
    m.lbegin(4, 12, 1);
    m.ebegin();
    m.lbegin(1, 12, (int)cols.size());
    for (size_t i = 0; i < cols.size(); i++) {
      m.ebegin();
      m.i64(2, metas[i].page_off);
      m.sbegin(3);
      m.i32(1, cols[i].ptype);
      m.lbegin(2, 5, 2);
      m.li32(0);
      m.li32(3);
      m.lbegin(3, 8, 1);
      m.lbin(cols[i].name);
      m.i32(4, 0);
      m.i64(5, nrows);
      m.i64(6, metas[i].total);
      m.i64(7, metas[i].total);
      m.i64(9, metas[i].page_off);
      m.send();
      m.eend();
    }
    m.i64(2, total_bytes);
    m.i64(3, nrows);
    m.eend();
  } else {
    m.lbegin(4, 12, 0);
  }
  m.bin(6, "shopping-sessions-native");
  m.byte(0);
  fwrite(m.b.data(), 1, m.b.size(), f);
  uint32_t ml = (uint32_t)m.b.size();
  fwrite(&ml, 1, 4, f);
  fwrite("PAR1", 1, 4, f);
  return fclose(f) == 0;
}

// Raw-buffer column (no copies): values/def levels point to caller memory.
struct RawColumn {
  std::string name;
  int ptype;
  int logical;
  bool optional;
  const char* vals;
  size_t vlen;
  const char* def;  // 4-byte length prefix + RLE data, optional columns only
  size_t dlen;
};

inline bool write_raw(const std::string& path, int64_t nrows, const std::vector<RawColumn>& cols) {
  FILE* f = fopen(path.c_str(), "wb");
  if (!f) return false;
  setvbuf(f, nullptr, _IONBF, 0);
  fwrite("PAR1", 1, 4, f);
  int64_t off = 4;
  struct Meta { int64_t page_off, total; };
  std::vector<Meta> metas;
  for (auto& c : cols) {
    if (nrows == 0) break;
    Thrift t;
    int64_t body = (int64_t)c.vlen + (int64_t)(c.optional ? c.dlen : 0);
    t.i32(1, 0);
    t.i32(2, (int32_t)body);
    t.i32(3, (int32_t)body);
    t.sbegin(5);
    t.i32(1, (int32_t)nrows);
    t.i32(2, 0);
    t.i32(3, 3);
    t.i32(4, 3);
    t.send();
    t.byte(0);
    fwrite(t.b.data(), 1, t.b.size(), f);
    if (c.optional) fwrite(c.def, 1, c.dlen, f);
    fwrite(c.vals, 1, c.vlen, f);
    int64_t total = (int64_t)t.b.size() + body;
    metas.push_back({off, total});
    off += total;
  }
  Thrift m;
  m.i32(1, 1);
  m.lbegin(2, 12, (int)cols.size() + 1);
  m.ebegin();
  m.bin(4, "schema");
  m.i32(5, (int32_t)cols.size());
  m.eend();
  for (auto& c : cols) {
    m.ebegin();
    m.i32(1, c.ptype);
    m.i32(3, c.optional ? 1 : 0);
    m.bin(4, c.name);
    if (c.logical == L_STRING) m.i32(6, 0);
    if (c.logical == L_DATE) m.i32(6, 6);
    if (c.logical != L_NONE) {
      m.sbegin(10);
      m.sbegin(c.logical);
      m.send();
      m.send();
    }
    m.eend();
  }
  m.i64(3, nrows);
  int64_t total_bytes = 0;
  for (auto& x : metas) total_bytes += x.total;
  if (nrows > 0) {
    m.lbegin(4, 12, 1);
    m.ebegin();
    m.lbegin(1, 12, (int)cols.size());
    for (size_t i = 0; i < cols.size(); i++) {
      m.ebegin();
      m.i64(2, metas[i].page_off);
      m.sbegin(3);
      m.i32(1, cols[i].ptype);
      m.lbegin(2, 5, 2);
      m.li32(0);
      m.li32(3);
      m.lbegin(3, 8, 1);
      m.lbin(cols[i].name);
      m.i32(4, 0);
      m.i64(5, nrows);
      m.i64(6, metas[i].total);
      m.i64(7, metas[i].total);
      m.i64(9, metas[i].page_off);
      m.send();
      m.eend();
    }
    m.i64(2, total_bytes);
    m.i64(3, nrows);
    m.eend();
  } else {
    m.lbegin(4, 12, 0);
  }
  m.bin(6, "shopping-sessions-native");
  m.byte(0);
  fwrite(m.b.data(), 1, m.b.size(), f);
  uint32_t ml = (uint32_t)m.b.size();
  fwrite(&ml, 1, 4, f);
  fwrite("PAR1", 1, 4, f);
  return fclose(f) == 0;
}

inline void put_i32(std::string& s, int32_t v) { s.append((const char*)&v, 4); }
inline void put_i64(std::string& s, int64_t v) { s.append((const char*)&v, 8); }
inline void put_str(std::string& s, const char* p, uint32_t n) {
  s.append((const char*)&n, 4);
  s.append(p, n);
}

}  // namespace pq
