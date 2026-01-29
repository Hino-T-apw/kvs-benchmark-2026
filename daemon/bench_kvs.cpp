// bench_kvs.cpp
// g++ -O3 -std=c++20 bench_kvs.cpp -pthread -o bench_kvs
//
// Examples:
//   ./bench_kvs --mode mixed --threads 64 --ops 200000 --value-size 256
//   ./bench_kvs --targets hinotetsu,memcached --mode set --threads 32 --ops 500000
//
// Defaults:
//   hinotetsu 127.0.0.1:11211
//   memcached 127.0.0.1:11212
//   redis     127.0.0.1:6379

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>

using Clock = std::chrono::steady_clock;

static inline void die(const std::string& msg) {
  std::cerr << "ERROR: " << msg << "\n";
  std::exit(1);
}

static inline bool starts_with(const std::string& s, const char* pfx) {
  return s.rfind(pfx, 0) == 0;
}

static inline uint64_t now_ns() {
  return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

static inline void set_tcp_nodelay(int fd) {
  int v = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &v, sizeof(v));
}

struct TcpConn {
  int fd{-1};
  std::string rbuf;

  ~TcpConn() { close(); }

  void close() {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
  }

  void connect_to(const std::string& host, int port) {
    close();
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = nullptr;
    std::string port_s = std::to_string(port);
    int rc = getaddrinfo(host.c_str(), port_s.c_str(), &hints, &res);
    if (rc != 0) die(std::string("getaddrinfo: ") + gai_strerror(rc));

    int sfd = -1;
    for (auto* p = res; p; p = p->ai_next) {
      sfd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
      if (sfd < 0) continue;
      if (::connect(sfd, p->ai_addr, p->ai_addrlen) == 0) break;
      ::close(sfd);
      sfd = -1;
    }
    freeaddrinfo(res);
    if (sfd < 0) die("connect failed to " + host + ":" + std::to_string(port));
    fd = sfd;
    set_tcp_nodelay(fd);
    rbuf.clear();
  }

  void send_all(const char* data, size_t n) {
    size_t off = 0;
    while (off < n) {
      ssize_t w = ::send(fd, data + off, n - off, 0);
      if (w <= 0) die("send failed");
      off += (size_t)w;
    }
  }

  void send_all(const std::string& s) {
    send_all(s.data(), s.size());
  }

  // Read until we have a '\n'. Returns line including '\n' (or '\r\n'), without stripping.
  std::string read_line() {
    while (true) {
      auto pos = rbuf.find('\n');
      if (pos != std::string::npos) {
        std::string line = rbuf.substr(0, pos + 1);
        rbuf.erase(0, pos + 1);
        return line;
      }
      char tmp[8192];
      ssize_t r = ::recv(fd, tmp, sizeof(tmp), 0);
      if (r <= 0) die("recv failed/closed");
      rbuf.append(tmp, (size_t)r);
    }
  }

  // Read exactly n bytes (binary), returns as string.
  std::string read_exact(size_t n) {
    while (rbuf.size() < n) {
      char tmp[8192];
      ssize_t r = ::recv(fd, tmp, sizeof(tmp), 0);
      if (r <= 0) die("recv failed/closed");
      rbuf.append(tmp, (size_t)r);
    }
    std::string out = rbuf.substr(0, n);
    rbuf.erase(0, n);
    return out;
  }
};

enum class Mode { SET, GET, MIXED };

struct Stats {
  uint64_t ops{0};
  uint64_t ns_total{0};
  std::vector<double> lat_ms; // optional; can be large. We'll sample if needed.
};

static inline double pct(const std::vector<double>& s, double p) {
  if (s.empty()) return 0.0;
  double k = (s.size() - 1) * p;
  size_t f = (size_t)k;
  size_t c = std::min(f + 1, s.size() - 1);
  if (f == c) return s[f];
  return s[f] + (s[c] - s[f]) * (k - (double)f);
}

static std::vector<std::string> make_keys(size_t keyspace, int key_len) {
  std::vector<std::string> keys;
  keys.reserve(keyspace);
  std::mt19937 rng(42);
  const char* alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<int> dist(0, 61);

  for (size_t i = 0; i < keyspace; i++) {
    std::string k;
    k.reserve((size_t)key_len);
    k.push_back('k');
    for (int j = 1; j < key_len; j++) k.push_back(alnum[dist(rng)]);
    keys.push_back(std::move(k));
  }
  return keys;
}

static std::string make_value(size_t n) {
  return std::string(n, 'x');
}

// ------------------------------
// Clients
// ------------------------------
struct HinotetsuClient {
  TcpConn c;

  void connect(const std::string& host, int port) { c.connect_to(host, port); }

  void set(const std::string& key, const std::string& value) {
    // set <key> <value>\n
    std::string cmd;
    cmd.reserve(4 + 1 + key.size() + 1 + value.size() + 1);
    cmd.append("set ");
    cmd.append(key);
    cmd.push_back(' ');
    cmd.append(value);
    cmd.append("\n");
    c.send_all(cmd);
    auto line = c.read_line();
    // allow \r\n too
    if (!(starts_with(line, "STORED"))) die("hinotetsu set failed: " + line);
  }

  std::string get(const std::string& key) {
    std::string cmd = "get " + key + "\n"; // ←ここ
    c.send_all(cmd);
    auto line = c.read_line();
    // strip \r\n
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) line.pop_back();
    if (line == "NOT_FOUND" || line == "END" || line.empty()) return {};
    return line;
  }
};

struct MemcachedClient {
  TcpConn c;
  int ttl{0};

  void connect(const std::string& host, int port) { c.connect_to(host, port); }

  void set(const std::string& key, const std::string& value) {
    // set key 0 ttl bytes\r\nvalue\r\n  を 1回のsendで送る（分割を減らして安定化）
    std::string msg;
    msg.reserve(64 + key.size() + value.size());
    msg += "set " + key + " 0 " + std::to_string(ttl) + " " + std::to_string(value.size()) + "\r\n";
    msg += value;
    msg += "\r\n";
    c.send_all(msg);

    auto line = c.read_line();
    if (!starts_with(line, "STORED")) die("memcached set failed: " + line);
  }


  std::string get(const std::string& key) {
    std::string cmd = "get " + key + "\r\n";
    c.send_all(cmd);

    auto first = c.read_line();
    if (starts_with(first, "END")) return {};

    if (!starts_with(first, "VALUE ")) {
      die("memcached bad get response: " + first);
    }

    // VALUE <key> <flags> <bytes>\r\n
    // parse bytes
    size_t last_space = first.find_last_of(' ');
    if (last_space == std::string::npos) die("memcached VALUE parse failed");
    size_t bytes = (size_t)std::stoul(first.substr(last_space + 1));

    std::string data = c.read_exact(bytes);
    // consume \r\n
    auto crlf = c.read_exact(2);
    (void)crlf;
    auto end = c.read_line();
    if (!starts_with(end, "END")) die("memcached expected END, got: " + end);
    return data;
  }
};

struct RedisClient {
  TcpConn c;
  int ttl{0};

  void connect(const std::string& host, int port) { c.connect_to(host, port); }

  static std::string resp_bulk(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
  }

  void set(const std::string& key, const std::string& value) {
    // SET key value  (optionally EX ttl)
    std::string req;
    if (ttl > 0) {
      // *5 SET key value EX ttl
      req.reserve(64 + key.size() + value.size());
      req += "*5\r\n";
      req += "$3\r\nSET\r\n";
      req += resp_bulk(key);
      req += resp_bulk(value);
      req += "$2\r\nEX\r\n";
      req += "$" + std::to_string(std::to_string(ttl).size()) + "\r\n" + std::to_string(ttl) + "\r\n";
    } else {
      // *3
      req.reserve(32 + key.size() + value.size());
      req += "*3\r\n";
      req += "$3\r\nSET\r\n";
      req += resp_bulk(key);
      req += resp_bulk(value);
    }
    c.send_all(req);
    auto line = c.read_line(); // +OK\r\n
    if (!starts_with(line, "+OK")) die("redis set failed: " + line);
  }

  std::string get(const std::string& key) {
    std::string req;
    req.reserve(32 + key.size());
    req += "*2\r\n";
    req += "$3\r\nGET\r\n";
    req += resp_bulk(key);
    c.send_all(req);

    auto line = c.read_line(); // $len\r\n or $-1\r\n
    if (line.empty()) die("redis get empty response");
    if (line[0] != '$') die("redis get unexpected: " + line);

    // strip \r\n for parsing
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) line.pop_back();
    long len = std::stol(line.substr(1));
    if (len < 0) return {};

    std::string data = c.read_exact((size_t)len);
    auto crlf = c.read_exact(2);
    (void)crlf;
    return data;
  }
};

// ------------------------------
// Runner
// ------------------------------
struct TargetCfg {
  std::string name;
  std::string host;
  int port;
};

struct Options {
  Mode mode{Mode::MIXED};
  uint64_t ops{200000};
  int threads{64};
  size_t keyspace{10000};
  int key_len{16};
  size_t value_size{256};
  int ttl{0}; // memcached/redis
  std::vector<std::string> targets{"hinotetsu","memcached","redis"};
  // latency sampling (store only 1/N)
  int lat_sample_every{1}; // 1 = store all; larger reduces memory
};

static Mode parse_mode(const std::string& s) {
  if (s == "set") return Mode::SET;
  if (s == "get") return Mode::GET;
  if (s == "mixed") return Mode::MIXED;
  die("bad mode: " + s);
  return Mode::MIXED;
}

static std::vector<std::string> split_csv(const std::string& s) {
  std::vector<std::string> out;
  std::string cur;
  for (char ch : s) {
    if (ch == ',') {
      if (!cur.empty()) out.push_back(cur);
      cur.clear();
    } else cur.push_back(ch);
  }
  if (!cur.empty()) out.push_back(cur);
  // trim spaces (minimal)
  for (auto& t : out) {
    while (!t.empty() && t.front()==' ') t.erase(t.begin());
    while (!t.empty() && t.back()==' ') t.pop_back();
  }
  return out;
}

static void parse_args(int argc, char** argv, Options& opt,
                       TargetCfg& hin, TargetCfg& mem, TargetCfg& red) {
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto need = [&](const char* k)->std::string{
      if (i + 1 >= argc) die(std::string("missing value for ") + k);
      return argv[++i];
    };

    if (a == "--mode") opt.mode = parse_mode(need("--mode"));
    else if (a == "--ops") opt.ops = std::stoull(need("--ops"));
    else if (a == "--threads") opt.threads = std::stoi(need("--threads"));
    else if (a == "--keyspace") opt.keyspace = (size_t)std::stoull(need("--keyspace"));
    else if (a == "--key-len") opt.key_len = std::stoi(need("--key-len"));
    else if (a == "--value-size") opt.value_size = (size_t)std::stoull(need("--value-size"));
    else if (a == "--ttl") opt.ttl = std::stoi(need("--ttl"));
    else if (a == "--targets") opt.targets = split_csv(need("--targets"));
    else if (a == "--lat-sample-every") opt.lat_sample_every = std::max(1, std::stoi(need("--lat-sample-every")));

    else if (a == "--hinotetsu-host") hin.host = need("--hinotetsu-host");
    else if (a == "--hinotetsu-port") hin.port = std::stoi(need("--hinotetsu-port"));
    else if (a == "--memcached-host") mem.host = need("--memcached-host");
    else if (a == "--memcached-port") mem.port = std::stoi(need("--memcached-port"));
    else if (a == "--redis-host") red.host = need("--redis-host");
    else if (a == "--redis-port") red.port = std::stoi(need("--redis-port"));

    else if (a == "-h" || a == "--help") {
      std::cout <<
R"(Usage: ./bench_kvs [options]

Options:
  --mode set|get|mixed          (default: mixed)
  --ops N                       total ops (default: 200000)
  --threads N                   concurrency via threads (default: 64)
  --keyspace N                  number of distinct keys (default: 10000)
  --key-len N                   key length (default: 16)
  --value-size N                value size in bytes (default: 256)
  --ttl N                       seconds for memcached/redis SET (default: 0)
  --targets csv                 hinotetsu,memcached,redis (default: all)
  --lat-sample-every N          store 1/N latencies (default: 1 = all)

  --hinotetsu-host HOST         (default 127.0.0.1)
  --hinotetsu-port PORT         (default 11211)
  --memcached-host HOST         (default 127.0.0.1)
  --memcached-port PORT         (default 11212)
  --redis-host HOST             (default 127.0.0.1)
  --redis-port PORT             (default 6379)
)";
      std::exit(0);
    }
    else {
      die("unknown arg: " + a);
    }
  }
}

struct BenchResult {
  std::string name;
  uint64_t ops{0};
  double seconds{0};
  double ops_per_sec{0};
  double avg_ms{0};
  double p50_ms{0};
  double p95_ms{0};
  double p99_ms{0};
};

static BenchResult summarize(const std::string& name, uint64_t ops, uint64_t ns_total, std::vector<double>& lats) {
  std::sort(lats.begin(), lats.end());
  double seconds = (double)ns_total / 1e9;
  BenchResult r;
  r.name = name;
  r.ops = ops;
  r.seconds = seconds;
  r.ops_per_sec = seconds > 0 ? (double)ops / seconds : 0.0;
  if (!lats.empty()) {
    double sum = 0;
    for (double v : lats) sum += v;
    r.avg_ms = sum / lats.size();
    r.p50_ms = pct(lats, 0.50);
    r.p95_ms = pct(lats, 0.95);
    r.p99_ms = pct(lats, 0.99);
  }
  return r;
}

static void print_results(const std::vector<BenchResult>& rs) {
  std::cout << "\n=== results ===\n";
  std::cout << "name\tops\tseconds\top/s\tavg_ms\tp50_ms\tp95_ms\tp99_ms\n";
  for (auto& r : rs) {
    std::cout << r.name << "\t"
              << r.ops << "\t"
              << r.seconds << "\t"
              << r.ops_per_sec << "\t"
              << r.avg_ms << "\t"
              << r.p50_ms << "\t"
              << r.p95_ms << "\t"
              << r.p99_ms << "\n";
  }
}

// Preload for GET/MIXED to avoid miss skew.
template <typename Client>
static void preload(Client& cli, const std::vector<std::string>& keys, const std::string& value, uint64_t n, std::mt19937& rng) {
  std::uniform_int_distribution<size_t> dk(0, keys.size() - 1);
  for (uint64_t i = 0; i < n; i++) {
    cli.set(keys[dk(rng)], value);
  }
}

static void run_memcached(const Options& opt, const TargetCfg& cfg,
                          const std::vector<std::string>& keys, const std::string& value,
                          std::vector<BenchResult>& out,
                          const std::string& label) {

  uint64_t ops_total = opt.ops;
  int T = opt.threads;
  uint64_t per = ops_total / (uint64_t)T;
  uint64_t rem = ops_total % (uint64_t)T;

  std::atomic<uint64_t> ns_sum{0};
  std::vector<std::vector<double>> lat_tls(T);

  if (opt.mode != Mode::SET) {
    MemcachedClient pre;
    pre.ttl = opt.ttl;
    pre.connect(cfg.host, cfg.port);
    std::mt19937 rng(999);
    preload(pre, keys, value, std::min<uint64_t>((uint64_t)keys.size(), 20000), rng);
  }

  std::vector<std::thread> th;
  th.reserve(T);
  for (int t = 0; t < T; t++) {
    uint64_t myops = per + (t < (int)rem ? 1 : 0);
    th.emplace_back([&, t, myops](){
      MemcachedClient cli;
      cli.ttl = opt.ttl;
      cli.connect(cfg.host, cfg.port);
      std::mt19937 rng(2000 + t);
      std::uniform_int_distribution<size_t> dk(0, keys.size() - 1);

      auto& lats = lat_tls[t];
      if (opt.lat_sample_every == 1) lats.reserve((size_t)myops);

      uint64_t t0 = now_ns();
      for (uint64_t i = 0; i < myops; i++) {
        const std::string& k = keys[dk(rng)];
        uint64_t op0 = now_ns();
        if (opt.mode == Mode::SET) {
          cli.set(k, value);
        } else if (opt.mode == Mode::GET) {
          (void)cli.get(k);
        } else {
          if ((i & 1) == 0) cli.set(k, value);
          else (void)cli.get(k);
        }
        uint64_t op1 = now_ns();
        if ((int)(i % (uint64_t)opt.lat_sample_every) == 0) {
          lats.push_back((double)(op1 - op0) / 1e6);
        }
      }
      uint64_t t1 = now_ns();
      ns_sum.fetch_add(t1 - t0, std::memory_order_relaxed);
    });
  }
  for (auto& x : th) x.join();

  std::vector<double> lats;
  size_t total_lat = 0;
  for (auto& v : lat_tls) total_lat += v.size();
  lats.reserve(total_lat);
  for (auto& v : lat_tls) lats.insert(lats.end(), v.begin(), v.end());
  std::string m = (opt.mode == Mode::SET ? "set" : opt.mode == Mode::GET ? "get" : "mixed");
  out.push_back(summarize(label + ":" + m, ops_total, ns_sum.load(), lats));
}

static void run_redis(const Options& opt, const TargetCfg& cfg,
                      const std::vector<std::string>& keys, const std::string& value,
                      std::vector<BenchResult>& out) {
  uint64_t ops_total = opt.ops;
  int T = opt.threads;
  uint64_t per = ops_total / (uint64_t)T;
  uint64_t rem = ops_total % (uint64_t)T;

  std::atomic<uint64_t> ns_sum{0};
  std::vector<std::vector<double>> lat_tls(T);

  if (opt.mode != Mode::SET) {
    RedisClient pre;
    pre.ttl = opt.ttl;
    pre.connect(cfg.host, cfg.port);
    std::mt19937 rng(999);
    preload(pre, keys, value, std::min<uint64_t>((uint64_t)keys.size(), 20000), rng);
  }

  std::vector<std::thread> th;
  th.reserve(T);
  for (int t = 0; t < T; t++) {
    uint64_t myops = per + (t < (int)rem ? 1 : 0);
    th.emplace_back([&, t, myops](){
      RedisClient cli;
      cli.ttl = opt.ttl;
      cli.connect(cfg.host, cfg.port);
      std::mt19937 rng(3000 + t);
      std::uniform_int_distribution<size_t> dk(0, keys.size() - 1);

      auto& lats = lat_tls[t];
      if (opt.lat_sample_every == 1) lats.reserve((size_t)myops);

      uint64_t t0 = now_ns();
      for (uint64_t i = 0; i < myops; i++) {
        const std::string& k = keys[dk(rng)];
        uint64_t op0 = now_ns();
        if (opt.mode == Mode::SET) {
          cli.set(k, value);
        } else if (opt.mode == Mode::GET) {
          (void)cli.get(k);
        } else {
          if ((i & 1) == 0) cli.set(k, value);
          else (void)cli.get(k);
        }
        uint64_t op1 = now_ns();
        if ((int)(i % (uint64_t)opt.lat_sample_every) == 0) {
          lats.push_back((double)(op1 - op0) / 1e6);
        }
      }
      uint64_t t1 = now_ns();
      ns_sum.fetch_add(t1 - t0, std::memory_order_relaxed);
    });
  }
  for (auto& x : th) x.join();

  std::vector<double> lats;
  size_t total_lat = 0;
  for (auto& v : lat_tls) total_lat += v.size();
  lats.reserve(total_lat);
  for (auto& v : lat_tls) lats.insert(lats.end(), v.begin(), v.end());

  std::string m = (opt.mode == Mode::SET ? "set" : opt.mode == Mode::GET ? "get" : "mixed");
  out.push_back(summarize("redis:" + m, ops_total, ns_sum.load(), lats));
}

int main(int argc, char** argv) {
  Options opt;
  TargetCfg hin{"hinotetsu","127.0.0.1",11211};
  TargetCfg mem{"memcached","127.0.0.1",11212};
  TargetCfg red{"redis","127.0.0.1",6379};

  parse_args(argc, argv, opt, hin, mem, red);

  auto keys = make_keys(opt.keyspace, opt.key_len);
  auto value = make_value(opt.value_size);

  std::vector<BenchResult> results;
  for (auto& t : opt.targets) {
    if (t == "hinotetsu") run_memcached(opt, hin, keys, value, results, "hinotetsu");
    else if (t == "memcached") run_memcached(opt, mem, keys, value, results, "memcached");
    else if (t == "redis") run_redis(opt, red, keys, value, results);
    else die("unknown target: " + t);
  }


  print_results(results);
  return 0;
}
