/*
 * Tokyo Cabinet vs 自作KVM ベンチマーク対決
 * 
 * インストール (Mac):
 *   brew install tokyo-cabinet
 * 
 * コンパイル:
 *   gcc -O3 -o bench_vs bench_vs.c -I/usr/local/include -L/usr/local/lib -ltokyocabinet -lm
 * 
 * 実行:
 *   ./bench_vs [件数(デフォルト:100000)]
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <tcutil.h>
#include <tchdb.h>

/* ========== 自作KVM ========== */
#define BUCKET_COUNT (8 * 1024)
#define BLOOM_SIZE (1 << 20)
#define POOL_SIZE (64 * 1024 * 1024)  /* 64MB（ローカルなので大きめ） */

#define BLOOM_OFF 0
#define BUCKET_OFF (BLOOM_SIZE / 8)
#define DATA_OFF (BUCKET_OFF + BUCKET_COUNT * sizeof(uint32_t))

typedef struct {
    uint32_t klen;
    uint32_t vlen;
    uint32_t next;
    char data[];
} Entry;

typedef struct {
    uint8_t *mem;
    size_t mem_size;
    uint8_t *bloom;
    uint32_t *buckets;
    size_t write_pos;
    size_t count;
} KVM;

static inline uint32_t fnv1a(const char *key, size_t len) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < len; i++)
        h = (h ^ (uint8_t)key[i]) * 16777619u;
    return h;
}

static inline uint32_t hash2(const char *key, size_t len) {
    uint32_t h = 0x5bd1e995;
    for (size_t i = 0; i < len; i++)
        h = ((h << 5) + h) ^ key[i];
    return h;
}

static inline uint32_t hash3(const char *key, size_t len) {
    uint32_t h = 0x811c9dc5;
    for (size_t i = 0; i < len; i++)
        h = (h * 31) + key[i];
    return h;
}

static inline void bloom_add(KVM *db, const char *key, size_t len) {
    uint32_t h1 = fnv1a(key, len) % BLOOM_SIZE;
    uint32_t h2 = hash2(key, len) % BLOOM_SIZE;
    uint32_t h3 = hash3(key, len) % BLOOM_SIZE;
    db->bloom[h1 >> 3] |= (1 << (h1 & 7));
    db->bloom[h2 >> 3] |= (1 << (h2 & 7));
    db->bloom[h3 >> 3] |= (1 << (h3 & 7));
}

static inline int bloom_maybe(KVM *db, const char *key, size_t len) {
    uint32_t h1 = fnv1a(key, len) % BLOOM_SIZE;
    uint32_t h2 = hash2(key, len) % BLOOM_SIZE;
    uint32_t h3 = hash3(key, len) % BLOOM_SIZE;
    return (db->bloom[h1 >> 3] & (1 << (h1 & 7))) &&
           (db->bloom[h2 >> 3] & (1 << (h2 & 7))) &&
           (db->bloom[h3 >> 3] & (1 << (h3 & 7)));
}

KVM *kvm_open() {
    KVM *db = calloc(1, sizeof(KVM));
    db->mem_size = POOL_SIZE;
    db->mem = mmap(NULL, db->mem_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (db->mem == MAP_FAILED) { db->mem = malloc(db->mem_size); }
    memset(db->mem, 0, DATA_OFF);
    db->bloom = db->mem + BLOOM_OFF;
    db->buckets = (uint32_t*)(db->mem + BUCKET_OFF);
    db->write_pos = DATA_OFF;
    return db;
}

void kvm_close(KVM *db) {
    if (db) { munmap(db->mem, db->mem_size); free(db); }
}

int kvm_put(KVM *db, const char *key, const char *value) {
    uint32_t klen = strlen(key), vlen = strlen(value);
    size_t entry_size = (sizeof(Entry) + klen + vlen + 7) & ~7;
    if (db->write_pos + entry_size > db->mem_size) return -1;
    uint32_t bucket = fnv1a(key, klen) % BUCKET_COUNT;
    Entry *e = (Entry*)(db->mem + db->write_pos);
    e->klen = klen; e->vlen = vlen; e->next = db->buckets[bucket];
    memcpy(e->data, key, klen);
    memcpy(e->data + klen, value, vlen);
    db->buckets[bucket] = db->write_pos;
    bloom_add(db, key, klen);
    db->write_pos += entry_size;
    db->count++;
    return 0;
}

char *kvm_get(KVM *db, const char *key) {
    uint32_t klen = strlen(key);
    if (!bloom_maybe(db, key, klen)) return NULL;
    uint32_t off = db->buckets[fnv1a(key, klen) % BUCKET_COUNT];
    while (off >= DATA_OFF) {
        Entry *e = (Entry*)(db->mem + off);
        if (e->klen == klen && memcmp(e->data, key, klen) == 0) {
            char *v = malloc(e->vlen + 1);
            memcpy(v, e->data + e->klen, e->vlen);
            v[e->vlen] = '\0';
            return v;
        }
        off = e->next;
    }
    return NULL;
}

/* ========== ベンチマーク ========== */
double now_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

void print_result(const char *name, const char *op, int n, double time) {
    printf("  %-12s | %-10s | %12.2f ops/sec | %.4f sec\n", name, op, n / time, time);
}

int main(int argc, char **argv) {
    int N = (argc > 1) ? atoi(argv[1]) : 100000;
    
    printf("╔══════════════════════════════════════════════════════════════════╗\n");
    printf("║       Tokyo Cabinet vs 自作KVM ベンチマーク対決                  ║\n");
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  Records: %-6d                                                 ║\n", N);
    printf("╚══════════════════════════════════════════════════════════════════╝\n\n");
    
    /* テストデータ生成 */
    char **keys = malloc(N * sizeof(char*));
    char **vals = malloc(N * sizeof(char*));
    char **miss = malloc(N * sizeof(char*));
    for (int i = 0; i < N; i++) {
        keys[i] = malloc(32); vals[i] = malloc(64); miss[i] = malloc(32);
        sprintf(keys[i], "key_%08d", i);
        sprintf(vals[i], "value_%d_data", i);
        sprintf(miss[i], "miss_%08d", i);
    }
    
    double tc_write, tc_seq, tc_rand, tc_miss;
    double kvm_write, kvm_seq, kvm_rand, kvm_miss;
    double t0;
    
    /* ========== Tokyo Cabinet ========== */
    printf(">>> Tokyo Cabinet (Hash DB)\n");
    remove("bench_tc.tch");
    
    TCHDB *hdb = tchdbnew();
    tchdbsetmutex(hdb);
    tchdbtune(hdb, N * 2, -1, -1, HDBTLARGE);  /* バケット数調整 */
    
    if (!tchdbopen(hdb, "bench_tc.tch", HDBOWRITER | HDBOCREAT | HDBOTRUNC)) {
        printf("Tokyo Cabinet open error: %s\n", tchdberrmsg(tchdbecode(hdb)));
        return 1;
    }
    
    /* TC Write */
    t0 = now_sec();
    for (int i = 0; i < N; i++)
        tchdbput2(hdb, keys[i], vals[i]);
    tchdbsync(hdb);
    tc_write = now_sec() - t0;
    
    /* TC Seq Read */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = tchdbget2(hdb, keys[i]);
        free(v);
    }
    tc_seq = now_sec() - t0;
    
    /* TC Rand Read */
    srand(12345);
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = tchdbget2(hdb, keys[rand() % N]);
        free(v);
    }
    tc_rand = now_sec() - t0;
    
    /* TC Miss Read */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = tchdbget2(hdb, miss[i]);
        free(v);
    }
    tc_miss = now_sec() - t0;
    
    tchdbclose(hdb);
    tchdbdel(hdb);
    
    int64_t tc_size = 0;
    tcstatfile("bench_tc.tch", NULL, &tc_size, NULL);
    printf("  File size: %.2f MB\n", (double)tc_size / 1024 / 1024);
    print_result("TokyoCabinet", "Write", N, tc_write);
    print_result("TokyoCabinet", "Seq Read", N, tc_seq);
    print_result("TokyoCabinet", "Rand Read", N, tc_rand);
    print_result("TokyoCabinet", "Miss Read", N, tc_miss);
    
    /* ========== 自作KVM ========== */
    printf("\n>>> 自作KVM (mmap + Bloom Filter)\n");
    
    KVM *kvm = kvm_open();
    
    /* KVM Write */
    t0 = now_sec();
    for (int i = 0; i < N; i++)
        kvm_put(kvm, keys[i], vals[i]);
    kvm_write = now_sec() - t0;
    
    /* KVM Seq Read */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = kvm_get(kvm, keys[i]);
        free(v);
    }
    kvm_seq = now_sec() - t0;
    
    /* KVM Rand Read */
    srand(12345);
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = kvm_get(kvm, keys[rand() % N]);
        free(v);
    }
    kvm_rand = now_sec() - t0;
    
    /* KVM Miss Read */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = kvm_get(kvm, miss[i]);
        free(v);
    }
    kvm_miss = now_sec() - t0;
    
    printf("  Memory used: %.2f MB\n", kvm->write_pos / (1024.0 * 1024.0));
    print_result("自作KVM", "Write", N, kvm_write);
    print_result("自作KVM", "Seq Read", N, kvm_seq);
    print_result("自作KVM", "Rand Read", N, kvm_rand);
    print_result("自作KVM", "Miss Read", N, kvm_miss);
    
    kvm_close(kvm);
    
    /* ========== 結果比較 ========== */
    printf("\n╔══════════════════════════════════════════════════════════════════╗\n");
    printf("║                        対決結果                                  ║\n");
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    printf("║  %-12s │ TokyoCabinet │  自作KVM   │  勝者        ║\n", "Operation");
    printf("╠══════════════════════════════════════════════════════════════════╣\n");
    
    #define WINNER(tc, kvm) ((tc) < (kvm) ? "TokyoCabinet" : "自作KVM ★")
    #define RATIO(tc, kvm) ((tc) < (kvm) ? (kvm)/(tc) : (tc)/(kvm))
    
    printf("║  %-12s │ %10.0f   │ %10.0f │  %-12s (%.1fx)\n", 
           "Write", N/tc_write, N/kvm_write, WINNER(tc_write, kvm_write), RATIO(tc_write, kvm_write));
    printf("║  %-12s │ %10.0f   │ %10.0f │  %-12s (%.1fx)\n", 
           "Seq Read", N/tc_seq, N/kvm_seq, WINNER(tc_seq, kvm_seq), RATIO(tc_seq, kvm_seq));
    printf("║  %-12s │ %10.0f   │ %10.0f │  %-12s (%.1fx)\n", 
           "Rand Read", N/tc_rand, N/kvm_rand, WINNER(tc_rand, kvm_rand), RATIO(tc_rand, kvm_rand));
    printf("║  %-12s │ %10.0f   │ %10.0f │  %-12s (%.1fx)\n", 
           "Miss Read", N/tc_miss, N/kvm_miss, WINNER(tc_miss, kvm_miss), RATIO(tc_miss, kvm_miss));
    printf("╚══════════════════════════════════════════════════════════════════╝\n");
    
    /* 総合判定 */
    int kvm_wins = 0;
    if (kvm_write < tc_write) kvm_wins++;
    if (kvm_seq < tc_seq) kvm_wins++;
    if (kvm_rand < tc_rand) kvm_wins++;
    if (kvm_miss < tc_miss) kvm_wins++;
    
    printf("\n🏆 総合結果: %s の勝利！ (%d - %d)\n", 
           kvm_wins >= 2 ? "自作KVM" : "Tokyo Cabinet", 
           kvm_wins, 4 - kvm_wins);
    
    /* クリーンアップ */
    remove("bench_tc.tch");
    for (int i = 0; i < N; i++) { free(keys[i]); free(vals[i]); free(miss[i]); }
    free(keys); free(vals); free(miss);
    
    return 0;
}