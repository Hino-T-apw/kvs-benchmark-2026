/*
 * KVS Hybrid - Hash ↔ RBTree 自動切替
 * 
 * 戦略:
 * - 少量(< THRESHOLD): Hashテーブル（高速）
 * - 大量(>= THRESHOLD): RBTree（スケーラブル）
 * - 閾値超えたら自動でHash→RBTree変換
 * 
 * コンパイル: gcc -O3 -o kvs_hybrid kvs_hybrid.c -lm
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/time.h>

/* 設定 */
#define POOL_SIZE (128 * 1024 * 1024)
#define HASH_BUCKETS (8 * 1024)
#define THRESHOLD (HASH_BUCKETS * 8)  /* 65536件でRBTreeに切替 */

/* Bloom動的拡張設定 */
#define BLOOM_INIT_BITS (1 << 20)     /* 初期: 1Mビット */
#define BLOOM_MAX_BITS (1 << 26)      /* 最大: 64Mビット = 8MB */
#define BLOOM_EXPAND_THRESHOLD 0.5    /* 充填率50%で拡張 */

#define RED 0
#define BLACK 1

/* ===== 共通Entry ===== */
typedef struct Entry {
    char *key;
    char *value;
    uint32_t klen, vlen;
    struct Entry *hash_next;  /* Hash用 */
} Entry;

/* ===== Red-Black Tree Node ===== */
typedef struct RBNode {
    Entry *entry;
    int color;
    struct RBNode *left, *right, *parent;
} RBNode;

typedef struct {
    RBNode *root;
    RBNode *nil;
} RBTree;

/* ===== KVS ===== */
typedef enum { MODE_HASH, MODE_RBTREE } KVSMode;

typedef struct {
    /* メモリプール */
    uint8_t *pool;
    size_t pool_pos;
    
    /* モード */
    KVSMode mode;
    size_t count;
    
    /* Hash用 */
    Entry **buckets;
    
    /* 動的Bloom Filter */
    uint8_t *bloom;
    size_t bloom_bits;      /* 現在のビット数 */
    size_t bloom_set_bits;  /* セットされたビット数(推定) */
    
    /* RBTree用 */
    RBTree *tree;
} KVS;

/* ===== Hash関数 ===== */
static inline uint32_t fnv1a(const char *key, size_t len) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < len; i++)
        h = (h ^ (uint8_t)key[i]) * 16777619u;
    return h;
}

static inline uint32_t hash2(const char *k, size_t l) {
    uint32_t h = 0x5bd1e995;
    for (size_t i = 0; i < l; i++) h = ((h << 5) + h) ^ k[i];
    return h;
}

static inline uint32_t hash3(const char *k, size_t l) {
    uint32_t h = 0x811c9dc5;
    for (size_t i = 0; i < l; i++) h = (h * 31) + k[i];
    return h;
}

/* ===== Bloom Filter (動的拡張対応) ===== */
static inline void bloom_set_bit(KVS *db, size_t pos) {
    size_t idx = pos >> 3;
    uint8_t mask = 1 << (pos & 7);
    if (!(db->bloom[idx] & mask)) {
        db->bloom[idx] |= mask;
        db->bloom_set_bits++;
    }
}

static inline int bloom_get_bit(KVS *db, size_t pos) {
    return db->bloom[pos >> 3] & (1 << (pos & 7));
}

static inline void bloom_add(KVS *db, const char *k, size_t l) {
    size_t bits = db->bloom_bits;
    bloom_set_bit(db, fnv1a(k, l) % bits);
    bloom_set_bit(db, hash2(k, l) % bits);
    bloom_set_bit(db, hash3(k, l) % bits);
}

static inline int bloom_maybe(KVS *db, const char *k, size_t l) {
    size_t bits = db->bloom_bits;
    return bloom_get_bit(db, fnv1a(k, l) % bits) &&
           bloom_get_bit(db, hash2(k, l) % bits) &&
           bloom_get_bit(db, hash3(k, l) % bits);
}

/* Bloom拡張: 全エントリを再ハッシュ */
void bloom_expand(KVS *db) {
    size_t new_bits = db->bloom_bits * 4;  /* 4倍に拡張 */
    if (new_bits > BLOOM_MAX_BITS) new_bits = BLOOM_MAX_BITS;
    if (new_bits == db->bloom_bits) return;  /* 最大到達 */
    
    printf("  [BLOOM] Expanding %zuK -> %zuK bits (%.1f%% full)\n",
           db->bloom_bits / 1024, new_bits / 1024,
           db->bloom_set_bits * 100.0 / db->bloom_bits);
    
    /* 新しいBloom確保 */
    size_t old_bits = db->bloom_bits;
    uint8_t *old_bloom = db->bloom;
    
    db->bloom = (uint8_t*)calloc(new_bits / 8, 1);
    db->bloom_bits = new_bits;
    db->bloom_set_bits = 0;
    
    /* 全エントリを再登録 */
    if (db->mode == MODE_HASH) {
        for (int i = 0; i < HASH_BUCKETS; i++) {
            Entry *e = db->buckets[i];
            while (e) {
                bloom_add(db, e->key, e->klen);
                e = e->hash_next;
            }
        }
    } else {
        /* RBTreeを走査して再登録 */
        /* インオーダー走査用スタック */
        RBNode *stack[64];
        int top = 0;
        RBNode *curr = db->tree->root;
        
        while (curr != db->tree->nil || top > 0) {
            while (curr != db->tree->nil) {
                stack[top++] = curr;
                curr = curr->left;
            }
            curr = stack[--top];
            bloom_add(db, curr->entry->key, curr->entry->klen);
            curr = curr->right;
        }
    }
    
    free(old_bloom);
    printf("  [BLOOM] Rehashed %zu entries, new fill: %.1f%%\n",
           db->count, db->bloom_set_bits * 100.0 / db->bloom_bits);
}

/* Bloom充填率チェック */
void bloom_check_expand(KVS *db) {
    double fill_rate = (double)db->bloom_set_bits / db->bloom_bits;
    if (fill_rate >= BLOOM_EXPAND_THRESHOLD && db->bloom_bits < BLOOM_MAX_BITS) {
        bloom_expand(db);
    }
}

/* ===== RBTree操作 ===== */
RBTree *rbtree_new() {
    RBTree *t = (RBTree*)calloc(1, sizeof(RBTree));
    t->nil = (RBNode*)calloc(1, sizeof(RBNode));
    t->nil->color = BLACK;
    t->root = t->nil;
    return t;
}

void rbtree_left_rotate(RBTree *t, RBNode *x) {
    RBNode *y = x->right;
    x->right = y->left;
    if (y->left != t->nil) y->left->parent = x;
    y->parent = x->parent;
    if (x->parent == t->nil) t->root = y;
    else if (x == x->parent->left) x->parent->left = y;
    else x->parent->right = y;
    y->left = x;
    x->parent = y;
}

void rbtree_right_rotate(RBTree *t, RBNode *x) {
    RBNode *y = x->left;
    x->left = y->right;
    if (y->right != t->nil) y->right->parent = x;
    y->parent = x->parent;
    if (x->parent == t->nil) t->root = y;
    else if (x == x->parent->right) x->parent->right = y;
    else x->parent->left = y;
    y->right = x;
    x->parent = y;
}

void rbtree_insert_fixup(RBTree *t, RBNode *z) {
    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            RBNode *y = z->parent->parent->right;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            } else {
                if (z == z->parent->right) { z = z->parent; rbtree_left_rotate(t, z); }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rbtree_right_rotate(t, z->parent->parent);
            }
        } else {
            RBNode *y = z->parent->parent->left;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            } else {
                if (z == z->parent->left) { z = z->parent; rbtree_right_rotate(t, z); }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rbtree_left_rotate(t, z->parent->parent);
            }
        }
    }
    t->root->color = BLACK;
}

void rbtree_insert(RBTree *t, RBNode *z) {
    RBNode *y = t->nil, *x = t->root;
    while (x != t->nil) {
        y = x;
        int cmp = strcmp(z->entry->key, x->entry->key);
        if (cmp < 0) x = x->left;
        else if (cmp > 0) x = x->right;
        else { x->entry = z->entry; free(z); return; }  /* 更新 */
    }
    z->parent = y;
    if (y == t->nil) t->root = z;
    else if (strcmp(z->entry->key, y->entry->key) < 0) y->left = z;
    else y->right = z;
    z->left = z->right = t->nil;
    z->color = RED;
    rbtree_insert_fixup(t, z);
}

RBNode *rbtree_search(RBTree *t, const char *key) {
    RBNode *x = t->root;
    while (x != t->nil) {
        int cmp = strcmp(key, x->entry->key);
        if (cmp == 0) return x;
        x = (cmp < 0) ? x->left : x->right;
    }
    return NULL;
}

void rbtree_free_nodes(RBTree *t, RBNode *n) {
    if (n != t->nil) {
        rbtree_free_nodes(t, n->left);
        rbtree_free_nodes(t, n->right);
        free(n);
    }
}

void rbtree_free(RBTree *t) {
    if (t) { rbtree_free_nodes(t, t->root); free(t->nil); free(t); }
}

/* ===== KVS操作 ===== */
void *pool_alloc(KVS *db, size_t size) {
    size = (size + 7) & ~7;
    if (db->pool_pos + size > POOL_SIZE) return NULL;
    void *p = db->pool + db->pool_pos;
    db->pool_pos += size;
    return p;
}

KVS *kvs_open() {
    KVS *db = (KVS*)calloc(1, sizeof(KVS));
    db->pool = (uint8_t*)mmap(NULL, POOL_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
    if (db->pool == MAP_FAILED) db->pool = (uint8_t*)malloc(POOL_SIZE);
    db->buckets = (Entry**)calloc(HASH_BUCKETS, sizeof(Entry*));
    
    /* 動的Bloom初期化 */
    db->bloom_bits = BLOOM_INIT_BITS;
    db->bloom = (uint8_t*)calloc(db->bloom_bits / 8, 1);
    db->bloom_set_bits = 0;
    
    db->tree = rbtree_new();
    db->mode = MODE_HASH;
    return db;
}

void kvs_close(KVS *db) {
    if (db) {
        free(db->buckets);
        free(db->bloom);
        rbtree_free(db->tree);
        munmap(db->pool, POOL_SIZE);
        free(db);
    }
}

/* Hash→RBTree変換 */
void kvs_convert_to_rbtree(KVS *db) {
    printf("  [AUTO] Converting Hash -> RBTree at %zu entries...\n", db->count);
    
    for (int i = 0; i < HASH_BUCKETS; i++) {
        Entry *e = db->buckets[i];
        while (e) {
            RBNode *node = (RBNode*)calloc(1, sizeof(RBNode));
            node->entry = e;
            rbtree_insert(db->tree, node);
            e = e->hash_next;
        }
        db->buckets[i] = NULL;
    }
    db->mode = MODE_RBTREE;
}

/* Write */
int kvs_put(KVS *db, const char *key, const char *value) {
    uint32_t klen = strlen(key), vlen = strlen(value);
    
    Entry *e = (Entry*)pool_alloc(db, sizeof(Entry));
    char *k = (char*)pool_alloc(db, klen + 1);
    char *v = (char*)pool_alloc(db, vlen + 1);
    if (!e || !k || !v) return -1;
    
    memcpy(k, key, klen + 1);
    memcpy(v, value, vlen + 1);
    e->key = k; e->value = v;
    e->klen = klen; e->vlen = vlen;
    e->hash_next = NULL;
    
    bloom_add(db, key, klen);
    
    if (db->mode == MODE_HASH) {
        uint32_t bucket = fnv1a(key, klen) % HASH_BUCKETS;
        e->hash_next = db->buckets[bucket];
        db->buckets[bucket] = e;
        db->count++;
        
        /* 閾値チェック */
        if (db->count >= THRESHOLD) {
            kvs_convert_to_rbtree(db);
        }
    } else {
        RBNode *node = (RBNode*)calloc(1, sizeof(RBNode));
        node->entry = e;
        rbtree_insert(db->tree, node);
        db->count++;
    }
    
    /* Bloom拡張チェック (1000件ごと) */
    if (db->count % 1000 == 0) {
        bloom_check_expand(db);
    }
    
    return 0;
}

/* Read */
char *kvs_get(KVS *db, const char *key) {
    uint32_t klen = strlen(key);
    
    /* Bloom Filter */
    if (!bloom_maybe(db, key, klen)) return NULL;
    
    if (db->mode == MODE_HASH) {
        uint32_t bucket = fnv1a(key, klen) % HASH_BUCKETS;
        Entry *e = db->buckets[bucket];
        while (e) {
            if (e->klen == klen && memcmp(e->key, key, klen) == 0) {
                char *v = (char*)malloc(e->vlen + 1);
                memcpy(v, e->value, e->vlen + 1);
                return v;
            }
            e = e->hash_next;
        }
    } else {
        RBNode *node = rbtree_search(db->tree, key);
        if (node) {
            char *v = (char*)malloc(node->entry->vlen + 1);
            memcpy(v, node->entry->value, node->entry->vlen + 1);
            return v;
        }
    }
    return NULL;
}

const char *kvs_mode_str(KVS *db) {
    return db->mode == MODE_HASH ? "Hash" : "RBTree";
}

/* ===== ベンチマーク ===== */
double now_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

void run_benchmark(int N) {
    printf("\n════════════════════════════════════════════════════════\n");
    printf("  Records: %d (Threshold: %d)\n", N, THRESHOLD);
    printf("════════════════════════════════════════════════════════\n\n");
    
    char **keys = (char**)malloc(N * sizeof(char*));
    char **vals = (char**)malloc(N * sizeof(char*));
    char **miss = (char**)malloc(N * sizeof(char*));
    for (int i = 0; i < N; i++) {
        keys[i] = (char*)malloc(32);
        vals[i] = (char*)malloc(64);
        miss[i] = (char*)malloc(32);
        sprintf(keys[i], "key_%08d", i);
        sprintf(vals[i], "value_%d_data", i);
        sprintf(miss[i], "miss_%08d", i);
    }
    
    KVS *db = kvs_open();
    double t0;
    
    /* Write */
    printf("Write...\n");
    t0 = now_sec();
    for (int i = 0; i < N; i++) kvs_put(db, keys[i], vals[i]);
    double write_time = now_sec() - t0;
    printf("  Final mode: %s\n", kvs_mode_str(db));
    printf("  %.2f ops/sec (%.4f sec)\n\n", N / write_time, write_time);
    
    /* Seq Read */
    printf("Sequential Read (%s)...\n", kvs_mode_str(db));
    t0 = now_sec();
    for (int i = 0; i < N; i++) { char *v = kvs_get(db, keys[i]); free(v); }
    double seq_time = now_sec() - t0;
    printf("  %.2f ops/sec (%.4f sec)\n\n", N / seq_time, seq_time);
    
    /* Random Read */
    printf("Random Read (%s)...\n", kvs_mode_str(db));
    srand(12345);
    t0 = now_sec();
    for (int i = 0; i < N; i++) { char *v = kvs_get(db, keys[rand() % N]); free(v); }
    double rand_time = now_sec() - t0;
    printf("  %.2f ops/sec (%.4f sec)\n\n", N / rand_time, rand_time);
    
    /* Miss Read */
    printf("Miss Read (%s + Bloom)...\n", kvs_mode_str(db));
    t0 = now_sec();
    for (int i = 0; i < N; i++) { char *v = kvs_get(db, miss[i]); free(v); }
    double miss_time = now_sec() - t0;
    printf("  %.2f ops/sec (%.4f sec)\n\n", N / miss_time, miss_time);
    
    printf("─────────────────────────────────────────────────────────\n");
    printf("  Summary (%d records, mode: %s)\n", N, kvs_mode_str(db));
    printf("─────────────────────────────────────────────────────────\n");
    printf("  %-12s | %12.2f ops/sec\n", "Write", N / write_time);
    printf("  %-12s | %12.2f ops/sec\n", "Seq Read", N / seq_time);
    printf("  %-12s | %12.2f ops/sec\n", "Rand Read", N / rand_time);
    printf("  %-12s | %12.2f ops/sec\n", "Miss Read", N / miss_time);
    printf("  %-12s | %12.2f MB\n", "Memory", db->pool_pos / (1024.0 * 1024.0));
    printf("  %-12s | %12zuK bits (%.1f%% full)\n", "Bloom",
           db->bloom_bits / 1024,
           db->bloom_set_bits * 100.0 / db->bloom_bits);
    
    kvs_close(db);
    for (int i = 0; i < N; i++) { free(keys[i]); free(vals[i]); free(miss[i]); }
    free(keys); free(vals); free(miss);
}

int main(int argc, char **argv) {
    printf("╔═══════════════════════════════════════════════════════════╗\n");
    printf("║   KVS Hybrid (Hash ↔ RBTree + Dynamic Bloom)              ║\n");
    printf("╠═══════════════════════════════════════════════════════════╣\n");
    printf("║  < %d entries  → Hash + Bloom (fast)              ║\n", THRESHOLD);
    printf("║  >= %d entries → RBTree + Bloom (scalable)        ║\n", THRESHOLD);
    printf("║  Bloom: %dK → %dK bits (auto expand at 50%%)        ║\n",
           BLOOM_INIT_BITS / 1024, BLOOM_MAX_BITS / 1024);
    printf("╚═══════════════════════════════════════════════════════════╝\n");
    
    if (argc > 1) {
        run_benchmark(atoi(argv[1]));
    } else {
        /* 両モードをテスト */
        run_benchmark(10000);    /* Hash mode */
        run_benchmark(100000);   /* RBTree mode */
        run_benchmark(1000000);  /* RBTree mode (大規模) */
    }
    
    return 0;
}