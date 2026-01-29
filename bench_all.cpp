/*
 * 4つ巴ベンチマーク対決
 * - 自作KVM
 * - Tokyo Cabinet
 * - Kyoto Cabinet  
 * - Tkrzw
 * 
 * インストール (Mac):
 *   brew install tokyo-cabinet kyoto-cabinet
 *   brew install tkrzw
 * 
 * コンパイル:
 *   g++ -O3 -std=c++17 -o bench_all bench_all.c \
 *       -I/usr/local/include \
 *       -L/usr/local/lib \
 *       -ltokyocabinet -lkyotocabinet -ltkrzw -lm
 * 
 * 実行:
 *   ./bench_all [件数(デフォルト:100000)]
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/time.h>

/* Tokyo Cabinet */
#include <tcutil.h>
#include <tchdb.h>

/* Kyoto Cabinet */
#include <kchashdb.h>

/* Tkrzw */
#include <tkrzw_dbm_hash.h>

/* ========== 自作KVM (100万件対応版) ========== */
#define BUCKET_COUNT (256 * 1024)   /* 256Kバケット → チェイン長約4 */
#define BLOOM_SIZE (1 << 24)        /* 16Mビット = 2MB */
#define POOL_SIZE (128 * 1024 * 1024) /* 128MB */

#define BLOOM_OFF 0
#define BUCKET_OFF (BLOOM_SIZE / 8)
#define DATA_OFF (BUCKET_OFF + BUCKET_COUNT * sizeof(uint32_t))

typedef struct { uint32_t klen, vlen, next; char data[]; } Entry;
typedef struct {
    uint8_t *mem; size_t mem_size;
    uint8_t *bloom; uint32_t *buckets;
    size_t write_pos, count;
} KVM;

static inline uint32_t fnv1a(const char *key, size_t len) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < len; i++) h = (h ^ (uint8_t)key[i]) * 16777619u;
    return h;
}
static inline uint32_t hash2(const char *k, size_t l) {
    uint32_t h = 0x5bd1e995; for (size_t i = 0; i < l; i++) h = ((h << 5) + h) ^ k[i]; return h;
}
static inline uint32_t hash3(const char *k, size_t l) {
    uint32_t h = 0x811c9dc5; for (size_t i = 0; i < l; i++) h = (h * 31) + k[i]; return h;
}
static inline void bloom_add(KVM *db, const char *k, size_t l) {
    db->bloom[(fnv1a(k,l) % BLOOM_SIZE) >> 3] |= 1 << (fnv1a(k,l) % 8);
    db->bloom[(hash2(k,l) % BLOOM_SIZE) >> 3] |= 1 << (hash2(k,l) % 8);
    db->bloom[(hash3(k,l) % BLOOM_SIZE) >> 3] |= 1 << (hash3(k,l) % 8);
}
static inline int bloom_maybe(KVM *db, const char *k, size_t l) {
    return (db->bloom[(fnv1a(k,l) % BLOOM_SIZE) >> 3] & (1 << (fnv1a(k,l) % 8))) &&
           (db->bloom[(hash2(k,l) % BLOOM_SIZE) >> 3] & (1 << (hash2(k,l) % 8))) &&
           (db->bloom[(hash3(k,l) % BLOOM_SIZE) >> 3] & (1 << (hash3(k,l) % 8)));
}

KVM *kvm_open() {
    KVM *db = (KVM*)calloc(1, sizeof(KVM));
    db->mem_size = POOL_SIZE;
    db->mem = (uint8_t*)mmap(NULL, db->mem_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
    if (db->mem == MAP_FAILED) db->mem = (uint8_t*)malloc(db->mem_size);
    memset(db->mem, 0, DATA_OFF);
    db->bloom = db->mem + BLOOM_OFF;
    db->buckets = (uint32_t*)(db->mem + BUCKET_OFF);
    db->write_pos = DATA_OFF;
    return db;
}
void kvm_close(KVM *db) { if (db) { munmap(db->mem, db->mem_size); free(db); } }

int kvm_put(KVM *db, const char *key, const char *val) {
    uint32_t kl = strlen(key), vl = strlen(val);
    size_t sz = (sizeof(Entry) + kl + vl + 7) & ~7;
    if (db->write_pos + sz > db->mem_size) return -1;
    uint32_t b = fnv1a(key, kl) % BUCKET_COUNT;
    Entry *e = (Entry*)(db->mem + db->write_pos);
    e->klen = kl; e->vlen = vl; e->next = db->buckets[b];
    memcpy(e->data, key, kl); memcpy(e->data + kl, val, vl);
    db->buckets[b] = db->write_pos;
    bloom_add(db, key, kl);
    db->write_pos += sz; db->count++;
    return 0;
}

char *kvm_get(KVM *db, const char *key) {
    uint32_t kl = strlen(key);
    if (!bloom_maybe(db, key, kl)) return NULL;
    uint32_t off = db->buckets[fnv1a(key, kl) % BUCKET_COUNT];
    while (off >= DATA_OFF) {
        Entry *e = (Entry*)(db->mem + off);
        if (e->klen == kl && memcmp(e->data, key, kl) == 0) {
            char *v = (char*)malloc(e->vlen + 1);
            memcpy(v, e->data + e->klen, e->vlen); v[e->vlen] = '\0';
            return v;
        }
        off = e->next;
    }
    return NULL;
}

/* ========== ベンチマーク ========== */
double now_sec() {
    struct timeval tv; gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

int main(int argc, char **argv) {
    int N = (argc > 1) ? atoi(argv[1]) : 100000;
    
    printf("╔═══════════════════════════════════════════════════════════════════════════╗\n");
    printf("║           KVM vs Tokyo Cabinet vs Kyoto Cabinet vs Tkrzw                  ║\n");
    printf("║                        4つ巴ベンチマーク対決                              ║\n");
    printf("╠═══════════════════════════════════════════════════════════════════════════╣\n");
    printf("║  Records: %-10d                                                       ║\n", N);
    printf("╚═══════════════════════════════════════════════════════════════════════════╝\n\n");
    
    char **keys = (char**)malloc(N * sizeof(char*));
    char **vals = (char**)malloc(N * sizeof(char*));
    char **miss = (char**)malloc(N * sizeof(char*));
    for (int i = 0; i < N; i++) {
        keys[i] = (char*)malloc(32); vals[i] = (char*)malloc(64); miss[i] = (char*)malloc(32);
        sprintf(keys[i], "key_%08d", i);
        sprintf(vals[i], "value_%d_data", i);
        sprintf(miss[i], "miss_%08d", i);
    }
    
    double results[4][4]; /* [db][op] */
    const char *db_names[] = {"自作KVM", "TokyoCabinet", "KyotoCabinet", "Tkrzw"};
    const char *op_names[] = {"Write", "Seq Read", "Rand Read", "Miss Read"};
    double t0;
    
    /* ========== 自作KVM ========== */
    printf(">>> 自作KVM\n");
    KVM *kvm = kvm_open();
    t0 = now_sec(); for (int i = 0; i < N; i++) kvm_put(kvm, keys[i], vals[i]); results[0][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvm_get(kvm, keys[i]); free(v); } results[0][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvm_get(kvm, keys[rand()%N]); free(v); } results[0][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvm_get(kvm, miss[i]); free(v); } results[0][3] = now_sec() - t0;
    printf("  Done. Memory: %.2f MB\n", kvm->write_pos / (1024.0 * 1024.0));
    kvm_close(kvm);
    
    /* ========== Tokyo Cabinet ========== */
    printf(">>> Tokyo Cabinet\n");
    remove("bench.tch");
    TCHDB *tdb = tchdbnew();
    tchdbtune(tdb, N * 2, -1, -1, HDBTLARGE);
    tchdbopen(tdb, "bench.tch", HDBOWRITER | HDBOCREAT | HDBOTRUNC);
    t0 = now_sec(); for (int i = 0; i < N; i++) tchdbput2(tdb, keys[i], vals[i]); tchdbsync(tdb); results[1][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = tchdbget2(tdb, keys[i]); free(v); } results[1][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = tchdbget2(tdb, keys[rand()%N]); free(v); } results[1][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = tchdbget2(tdb, miss[i]); free(v); } results[1][3] = now_sec() - t0;
    int64_t tc_size; tcstatfile("bench.tch", NULL, &tc_size, NULL);
    printf("  Done. File: %.2f MB\n", tc_size / (1024.0 * 1024.0));
    tchdbclose(tdb); tchdbdel(tdb);
    
    /* ========== Kyoto Cabinet ========== */
    printf(">>> Kyoto Cabinet\n");
    remove("bench.kch");
    kyotocabinet::HashDB kdb;
    kdb.tune_buckets(N * 2);
    kdb.open("bench.kch", kyotocabinet::HashDB::OWRITER | kyotocabinet::HashDB::OCREATE | kyotocabinet::HashDB::OTRUNCATE);
    t0 = now_sec(); for (int i = 0; i < N; i++) kdb.set(keys[i], vals[i]); kdb.synchronize(); results[2][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; kdb.get(keys[i], &v); } results[2][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; kdb.get(keys[rand()%N], &v); } results[2][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; kdb.get(miss[i], &v); } results[2][3] = now_sec() - t0;
    printf("  Done. File: %.2f MB\n", kdb.size() / (1024.0 * 1024.0));
    kdb.close();
    
    /* ========== Tkrzw ========== */
    printf(">>> Tkrzw\n");
    remove("bench.tkh");
    tkrzw::HashDBM tkdb;
    tkrzw::HashDBM::TuningParameters params;
    params.num_buckets = N * 2;
    tkdb.OpenAdvanced("bench.tkh", true, tkrzw::File::OPEN_TRUNCATE, params);
    t0 = now_sec(); for (int i = 0; i < N; i++) tkdb.Set(keys[i], vals[i]); tkdb.Synchronize(false); results[3][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; tkdb.Get(keys[i], &v); } results[3][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; tkdb.Get(keys[rand()%N], &v); } results[3][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; tkdb.Get(miss[i], &v); } results[3][3] = now_sec() - t0;
    printf("  Done. File: %.2f MB\n", tkdb.GetFileSizeSimple() / (1024.0 * 1024.0));
    tkdb.Close();
    
    /* ========== 結果表示 ========== */
    printf("\n╔════════════════════════════════════════════════════════════════════════════════════════╗\n");
    printf("║                                    対決結果 (ops/sec)                                   ║\n");
    printf("╠════════════════════════════════════════════════════════════════════════════════════════╣\n");
    printf("║ %-10s │ %14s │ %14s │ %14s │ %14s ║\n", "Operation", "自作KVM", "TokyoCabinet", "KyotoCabinet", "Tkrzw");
    printf("╠════════════════════════════════════════════════════════════════════════════════════════╣\n");
    
    int wins[4] = {0};
    for (int op = 0; op < 4; op++) {
        int best = 0;
        for (int db = 1; db < 4; db++) if (results[db][op] < results[best][op]) best = db;
        wins[best]++;
        
        printf("║ %-10s │", op_names[op]);
        for (int db = 0; db < 4; db++) {
            char buf[32];
            sprintf(buf, "%s%.0f", db == best ? "★" : " ", N / results[db][op]);
            printf(" %14s │", buf);
        }
        printf("\n");
    }
    printf("╚════════════════════════════════════════════════════════════════════════════════════════╝\n");
    
    /* 総合結果 */
    printf("\n🏆 総合結果:\n");
    for (int db = 0; db < 4; db++)
        printf("   %s: %d勝\n", db_names[db], wins[db]);
    
    int winner = 0;
    for (int db = 1; db < 4; db++) if (wins[db] > wins[winner]) winner = db;
    printf("\n   👑 優勝: %s!\n", db_names[winner]);
    
    /* クリーンアップ */
    remove("bench.tch"); remove("bench.kch"); remove("bench.tkh");
    for (int i = 0; i < N; i++) { free(keys[i]); free(vals[i]); free(miss[i]); }
    free(keys); free(vals); free(miss);
    
    return 0;
}