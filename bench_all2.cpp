/*
 * 究極6つ巴ベンチマーク対決
 * - 自作KVS (RBTree版)
 * - 自作KVS (B+Tree版)
 * - Tokyo Cabinet
 * - Kyoto Cabinet
 * - Tkrzw
 * - Berkeley DB
 * 
 * インストール (Mac):
 *   brew install tokyo-cabinet kyoto-cabinet tkrzw berkeley-db
 * 
 * コンパイル:
 *   g++ -O3 -std=c++17 -o ultimate_bench ultimate_bench.cpp \
 *       -I/usr/local/include -I/usr/local/opt/berkeley-db/include \
 *       -L/usr/local/lib -L/usr/local/opt/berkeley-db/lib \
 *       -ltokyocabinet -lkyotocabinet -ltkrzw -ldb -lm
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

/* Berkeley DB */
#include <db.h>

/*
 * ============================================================
 * 自作KVS共通部分
 * ============================================================
 */
#define POOL_SIZE (128 * 1024 * 1024)
#define BLOOM_INIT_BITS (1 << 20)
#define BLOOM_MAX_BITS (1 << 26)
#define BLOOM_EXPAND_THRESHOLD 0.5

/* Hash Functions */
static inline uint32_t fnv1a(const char *key, size_t len) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < len; i++) h = (h ^ (uint8_t)key[i]) * 16777619u;
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

static inline int keycmp(const char *k1, size_t l1, const char *k2, size_t l2) {
    size_t min = l1 < l2 ? l1 : l2;
    int cmp = memcmp(k1, k2, min);
    if (cmp != 0) return cmp;
    return (l1 < l2) ? -1 : (l1 > l2) ? 1 : 0;
}

/*
 * ============================================================
 * RBTree版KVS
 * ============================================================
 */
#define RB_BUCKET_COUNT (256 * 1024)
#define RED 0
#define BLACK 1

typedef struct RBEntry { char *key, *value; uint32_t klen, vlen; struct RBEntry *hash_next; } RBEntry;
typedef struct RBNode { RBEntry *entry; int color; struct RBNode *left, *right, *parent; } RBNode;
typedef struct RBTree { RBNode *root, *nil; } RBTree;

typedef struct {
    uint8_t *pool; size_t pool_pos;
    RBEntry **buckets; size_t bucket_count, threshold;
    uint8_t *bloom; size_t bloom_bits, bloom_set_bits;
    RBTree *tree; int mode; size_t count;
} KVS_RB;

static void rb_bloom_set(KVS_RB *db, size_t pos) {
    size_t idx = pos >> 3; uint8_t mask = 1 << (pos & 7);
    if (!(db->bloom[idx] & mask)) { db->bloom[idx] |= mask; db->bloom_set_bits++; }
}
static void rb_bloom_add(KVS_RB *db, const char *k, size_t l) {
    rb_bloom_set(db, fnv1a(k,l) % db->bloom_bits);
    rb_bloom_set(db, hash2(k,l) % db->bloom_bits);
    rb_bloom_set(db, hash3(k,l) % db->bloom_bits);
}
static int rb_bloom_maybe(KVS_RB *db, const char *k, size_t l) {
    return (db->bloom[(fnv1a(k,l) % db->bloom_bits) >> 3] & (1 << (fnv1a(k,l) % db->bloom_bits & 7))) &&
           (db->bloom[(hash2(k,l) % db->bloom_bits) >> 3] & (1 << (hash2(k,l) % db->bloom_bits & 7))) &&
           (db->bloom[(hash3(k,l) % db->bloom_bits) >> 3] & (1 << (hash3(k,l) % db->bloom_bits & 7)));
}

static RBTree *rbtree_new() {
    RBTree *t = (RBTree*)calloc(1, sizeof(RBTree));
    t->nil = (RBNode*)calloc(1, sizeof(RBNode)); t->nil->color = BLACK; t->root = t->nil;
    return t;
}
static void rb_left_rotate(RBTree *t, RBNode *x) {
    RBNode *y = x->right; x->right = y->left;
    if (y->left != t->nil) y->left->parent = x;
    y->parent = x->parent;
    if (x->parent == t->nil) t->root = y;
    else if (x == x->parent->left) x->parent->left = y;
    else x->parent->right = y;
    y->left = x; x->parent = y;
}
static void rb_right_rotate(RBTree *t, RBNode *x) {
    RBNode *y = x->left; x->left = y->right;
    if (y->right != t->nil) y->right->parent = x;
    y->parent = x->parent;
    if (x->parent == t->nil) t->root = y;
    else if (x == x->parent->right) x->parent->right = y;
    else x->parent->left = y;
    y->right = x; x->parent = y;
}
static void rb_insert_fixup(RBTree *t, RBNode *z) {
    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            RBNode *y = z->parent->parent->right;
            if (y->color == RED) { z->parent->color = BLACK; y->color = BLACK; z->parent->parent->color = RED; z = z->parent->parent; }
            else { if (z == z->parent->right) { z = z->parent; rb_left_rotate(t, z); } z->parent->color = BLACK; z->parent->parent->color = RED; rb_right_rotate(t, z->parent->parent); }
        } else {
            RBNode *y = z->parent->parent->left;
            if (y->color == RED) { z->parent->color = BLACK; y->color = BLACK; z->parent->parent->color = RED; z = z->parent->parent; }
            else { if (z == z->parent->left) { z = z->parent; rb_right_rotate(t, z); } z->parent->color = BLACK; z->parent->parent->color = RED; rb_left_rotate(t, z->parent->parent); }
        }
    }
    t->root->color = BLACK;
}
static void rbtree_insert(RBTree *t, RBNode *z) {
    RBNode *y = t->nil, *x = t->root;
    while (x != t->nil) { y = x; int c = keycmp(z->entry->key, z->entry->klen, x->entry->key, x->entry->klen); if (c < 0) x = x->left; else if (c > 0) x = x->right; else { x->entry = z->entry; free(z); return; } }
    z->parent = y; if (y == t->nil) t->root = z; else if (keycmp(z->entry->key, z->entry->klen, y->entry->key, y->entry->klen) < 0) y->left = z; else y->right = z;
    z->left = z->right = t->nil; z->color = RED; rb_insert_fixup(t, z);
}
static RBNode *rbtree_search(RBTree *t, const char *k, size_t l) {
    RBNode *x = t->root;
    while (x != t->nil) { int c = keycmp(k, l, x->entry->key, x->entry->klen); if (c == 0) return x; x = (c < 0) ? x->left : x->right; }
    return NULL;
}

static void *rb_pool_alloc(KVS_RB *db, size_t size) { size = (size + 7) & ~7; if (db->pool_pos + size > POOL_SIZE) return NULL; void *p = db->pool + db->pool_pos; db->pool_pos += size; return p; }

static void rb_convert_to_tree(KVS_RB *db) {
    for (size_t i = 0; i < db->bucket_count; i++) {
        RBEntry *e = db->buckets[i];
        while (e) { RBNode *n = (RBNode*)calloc(1, sizeof(RBNode)); n->entry = e; rbtree_insert(db->tree, n); e = e->hash_next; }
        db->buckets[i] = NULL;
    }
    db->mode = 1;
}

KVS_RB *kvs_rb_open() {
    KVS_RB *db = (KVS_RB*)calloc(1, sizeof(KVS_RB));
    db->pool = (uint8_t*)mmap(NULL, POOL_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
    db->bucket_count = RB_BUCKET_COUNT; db->threshold = RB_BUCKET_COUNT * 4;
    db->buckets = (RBEntry**)calloc(db->bucket_count, sizeof(RBEntry*));
    db->bloom_bits = BLOOM_INIT_BITS; db->bloom = (uint8_t*)calloc(db->bloom_bits / 8, 1);
    db->tree = rbtree_new();
    return db;
}
void kvs_rb_close(KVS_RB *db) { free(db->buckets); free(db->bloom); munmap(db->pool, POOL_SIZE); free(db); }

int kvs_rb_put(KVS_RB *db, const char *key, const char *val) {
    uint32_t kl = strlen(key), vl = strlen(val);
    RBEntry *e = (RBEntry*)rb_pool_alloc(db, sizeof(RBEntry));
    char *k = (char*)rb_pool_alloc(db, kl+1), *v = (char*)rb_pool_alloc(db, vl+1);
    memcpy(k, key, kl+1); memcpy(v, val, vl+1);
    e->key = k; e->value = v; e->klen = kl; e->vlen = vl; e->hash_next = NULL;
    rb_bloom_add(db, key, kl);
    if (db->mode == 0) {
        uint32_t b = fnv1a(key, kl) % db->bucket_count;
        e->hash_next = db->buckets[b]; db->buckets[b] = e; db->count++;
        if (db->count >= db->threshold) rb_convert_to_tree(db);
    } else {
        RBNode *n = (RBNode*)calloc(1, sizeof(RBNode)); n->entry = e; rbtree_insert(db->tree, n); db->count++;
    }
    return 0;
}
char *kvs_rb_get(KVS_RB *db, const char *key) {
    uint32_t kl = strlen(key);
    if (!rb_bloom_maybe(db, key, kl)) return NULL;
    if (db->mode == 0) {
        RBEntry *e = db->buckets[fnv1a(key, kl) % db->bucket_count];
        while (e) { if (e->klen == kl && memcmp(e->key, key, kl) == 0) { char *v = (char*)malloc(e->vlen+1); memcpy(v, e->value, e->vlen+1); return v; } e = e->hash_next; }
    } else {
        RBNode *n = rbtree_search(db->tree, key, kl);
        if (n) { char *v = (char*)malloc(n->entry->vlen+1); memcpy(v, n->entry->value, n->entry->vlen+1); return v; }
    }
    return NULL;
}

/*
 * ============================================================
 * B+Tree版KVS
 * ============================================================
 */
#define BP_ORDER 64

typedef struct { char *key, *value; uint32_t klen, vlen; } BPEntry;
typedef struct BPNode {
    int is_leaf, num_keys;
    char *keys[BP_ORDER-1]; uint32_t klens[BP_ORDER-1];
    union { struct BPNode *children[BP_ORDER]; BPEntry *entries[BP_ORDER-1]; };
    struct BPNode *next;
} BPNode;

typedef struct {
    uint8_t *pool; size_t pool_pos;
    BPNode *root; size_t count;
    uint8_t *bloom; size_t bloom_bits, bloom_set_bits;
} KVS_BP;

static void bp_bloom_set(KVS_BP *db, size_t pos) {
    size_t idx = pos >> 3; uint8_t mask = 1 << (pos & 7);
    if (!(db->bloom[idx] & mask)) { db->bloom[idx] |= mask; db->bloom_set_bits++; }
}
static void bp_bloom_add(KVS_BP *db, const char *k, size_t l) {
    bp_bloom_set(db, fnv1a(k,l) % db->bloom_bits);
    bp_bloom_set(db, hash2(k,l) % db->bloom_bits);
    bp_bloom_set(db, hash3(k,l) % db->bloom_bits);
}
static int bp_bloom_maybe(KVS_BP *db, const char *k, size_t l) {
    return (db->bloom[(fnv1a(k,l) % db->bloom_bits) >> 3] & (1 << (fnv1a(k,l) % db->bloom_bits & 7))) &&
           (db->bloom[(hash2(k,l) % db->bloom_bits) >> 3] & (1 << (hash2(k,l) % db->bloom_bits & 7))) &&
           (db->bloom[(hash3(k,l) % db->bloom_bits) >> 3] & (1 << (hash3(k,l) % db->bloom_bits & 7)));
}

static void *bp_pool_alloc(KVS_BP *db, size_t size) { size = (size + 7) & ~7; if (db->pool_pos + size > POOL_SIZE) return NULL; void *p = db->pool + db->pool_pos; db->pool_pos += size; return p; }
static BPNode *bp_node_new(int is_leaf) { BPNode *n = (BPNode*)calloc(1, sizeof(BPNode)); n->is_leaf = is_leaf; return n; }

static int bp_leaf_find(BPNode *n, const char *k, size_t l) {
    int lo = 0, hi = n->num_keys;
    while (lo < hi) { int m = (lo+hi)/2; if (keycmp(n->keys[m], n->klens[m], k, l) < 0) lo = m+1; else hi = m; }
    return lo;
}
static int bp_internal_find(BPNode *n, const char *k, size_t l) {
    int lo = 0, hi = n->num_keys;
    while (lo < hi) { int m = (lo+hi)/2; if (keycmp(n->keys[m], n->klens[m], k, l) <= 0) lo = m+1; else hi = m; }
    return lo;
}
static BPNode *bp_find_leaf(KVS_BP *db, const char *k, size_t l) {
    BPNode *n = db->root; while (n && !n->is_leaf) n = n->children[bp_internal_find(n, k, l)]; return n;
}

static void bp_split_leaf(KVS_BP *db, BPNode *leaf, BPNode *parent, int ppos);
static void bp_split_internal(KVS_BP *db, BPNode *node, BPNode *parent, int ppos);

static int bp_insert_rec(KVS_BP *db, BPNode *n, BPNode *parent, int ppos, const char *k, size_t kl, BPEntry *e) {
    if (n->is_leaf) {
        int pos = bp_leaf_find(n, k, kl);
        if (pos < n->num_keys && keycmp(n->keys[pos], n->klens[pos], k, kl) == 0) { n->entries[pos]->value = e->value; n->entries[pos]->vlen = e->vlen; return 0; }
        for (int i = n->num_keys; i > pos; i--) { n->keys[i] = n->keys[i-1]; n->klens[i] = n->klens[i-1]; n->entries[i] = n->entries[i-1]; }
        n->keys[pos] = e->key; n->klens[pos] = kl; n->entries[pos] = e; n->num_keys++;
        if (n->num_keys >= BP_ORDER-1) bp_split_leaf(db, n, parent, ppos);
        return 1;
    } else {
        int pos = bp_internal_find(n, k, kl);
        int added = bp_insert_rec(db, n->children[pos], n, pos, k, kl, e);
        if (n->num_keys >= BP_ORDER-1) bp_split_internal(db, n, parent, ppos);
        return added;
    }
}

static void bp_split_leaf(KVS_BP *db, BPNode *leaf, BPNode *parent, int ppos) {
    BPNode *nn = bp_node_new(1); int mid = leaf->num_keys / 2;
    nn->num_keys = leaf->num_keys - mid;
    for (int i = 0; i < nn->num_keys; i++) { nn->keys[i] = leaf->keys[mid+i]; nn->klens[i] = leaf->klens[mid+i]; nn->entries[i] = leaf->entries[mid+i]; }
    leaf->num_keys = mid; nn->next = leaf->next; leaf->next = nn;
    char *uk = nn->keys[0]; uint32_t ul = nn->klens[0];
    if (!parent) { BPNode *nr = bp_node_new(0); nr->num_keys = 1; nr->keys[0] = uk; nr->klens[0] = ul; nr->children[0] = leaf; nr->children[1] = nn; db->root = nr; }
    else { for (int i = parent->num_keys; i > ppos; i--) { parent->keys[i] = parent->keys[i-1]; parent->klens[i] = parent->klens[i-1]; parent->children[i+1] = parent->children[i]; } parent->keys[ppos] = uk; parent->klens[ppos] = ul; parent->children[ppos+1] = nn; parent->num_keys++; }
}
static void bp_split_internal(KVS_BP *db, BPNode *node, BPNode *parent, int ppos) {
    BPNode *nn = bp_node_new(0); int mid = node->num_keys / 2;
    char *uk = node->keys[mid]; uint32_t ul = node->klens[mid];
    nn->num_keys = node->num_keys - mid - 1;
    for (int i = 0; i < nn->num_keys; i++) { nn->keys[i] = node->keys[mid+1+i]; nn->klens[i] = node->klens[mid+1+i]; nn->children[i] = node->children[mid+1+i]; }
    nn->children[nn->num_keys] = node->children[node->num_keys]; node->num_keys = mid;
    if (!parent) { BPNode *nr = bp_node_new(0); nr->num_keys = 1; nr->keys[0] = uk; nr->klens[0] = ul; nr->children[0] = node; nr->children[1] = nn; db->root = nr; }
    else { for (int i = parent->num_keys; i > ppos; i--) { parent->keys[i] = parent->keys[i-1]; parent->klens[i] = parent->klens[i-1]; parent->children[i+1] = parent->children[i]; } parent->keys[ppos] = uk; parent->klens[ppos] = ul; parent->children[ppos+1] = nn; parent->num_keys++; }
}

KVS_BP *kvs_bp_open() {
    KVS_BP *db = (KVS_BP*)calloc(1, sizeof(KVS_BP));
    db->pool = (uint8_t*)mmap(NULL, POOL_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
    db->bloom_bits = BLOOM_INIT_BITS; db->bloom = (uint8_t*)calloc(db->bloom_bits / 8, 1);
    db->root = bp_node_new(1);
    return db;
}
void kvs_bp_close(KVS_BP *db) { free(db->bloom); munmap(db->pool, POOL_SIZE); free(db); }

int kvs_bp_put(KVS_BP *db, const char *key, const char *val) {
    uint32_t kl = strlen(key), vl = strlen(val);
    BPEntry *e = (BPEntry*)bp_pool_alloc(db, sizeof(BPEntry));
    char *k = (char*)bp_pool_alloc(db, kl+1), *v = (char*)bp_pool_alloc(db, vl+1);
    memcpy(k, key, kl+1); memcpy(v, val, vl+1);
    e->key = k; e->value = v; e->klen = kl; e->vlen = vl;
    bp_bloom_add(db, key, kl);
    if (bp_insert_rec(db, db->root, NULL, 0, key, kl, e)) db->count++;
    return 0;
}
char *kvs_bp_get(KVS_BP *db, const char *key) {
    uint32_t kl = strlen(key);
    if (!bp_bloom_maybe(db, key, kl)) return NULL;
    BPNode *leaf = bp_find_leaf(db, key, kl); if (!leaf) return NULL;
    int pos = bp_leaf_find(leaf, key, kl);
    if (pos < leaf->num_keys && keycmp(leaf->keys[pos], leaf->klens[pos], key, kl) == 0) {
        BPEntry *e = leaf->entries[pos]; char *v = (char*)malloc(e->vlen+1); memcpy(v, e->value, e->vlen+1); return v;
    }
    return NULL;
}

/*
 * ============================================================
 * ベンチマーク
 * ============================================================
 */
double now_sec() { struct timeval tv; gettimeofday(&tv, NULL); return tv.tv_sec + tv.tv_usec * 1e-6; }

int main(int argc, char **argv) {
    int N = (argc > 1) ? atoi(argv[1]) : 100000;
    
    printf("╔═══════════════════════════════════════════════════════════════════════════════════════════════════╗\n");
    printf("║                              究極6つ巴ベンチマーク対決                                            ║\n");
    printf("║  KVS(RBTree) vs KVS(B+Tree) vs TokyoCabinet vs KyotoCabinet vs Tkrzw vs BerkeleyDB               ║\n");
    printf("╠═══════════════════════════════════════════════════════════════════════════════════════════════════╣\n");
    printf("║  Records: %-10d                                                                               ║\n", N);
    printf("╚═══════════════════════════════════════════════════════════════════════════════════════════════════╝\n\n");
    
    char **keys = (char**)malloc(N * sizeof(char*));
    char **vals = (char**)malloc(N * sizeof(char*));
    char **miss = (char**)malloc(N * sizeof(char*));
    for (int i = 0; i < N; i++) {
        keys[i] = (char*)malloc(32); vals[i] = (char*)malloc(64); miss[i] = (char*)malloc(32);
        sprintf(keys[i], "key_%08d", i); sprintf(vals[i], "value_%d_data", i); sprintf(miss[i], "miss_%08d", i);
    }
    
    double results[6][4];
    const char *names[] = {"KVS(RBTree)", "KVS(B+Tree)", "TokyoCabinet", "KyotoCabinet", "Tkrzw", "BerkeleyDB"};
    const char *ops[] = {"Write", "Seq Read", "Rand Read", "Miss Read"};
    double t0;
    
    /* 1. KVS RBTree */
    printf(">>> KVS (RBTree)\n");
    KVS_RB *rb = kvs_rb_open();
    t0 = now_sec(); for (int i = 0; i < N; i++) kvs_rb_put(rb, keys[i], vals[i]); results[0][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvs_rb_get(rb, keys[i]); free(v); } results[0][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvs_rb_get(rb, keys[rand()%N]); free(v); } results[0][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvs_rb_get(rb, miss[i]); free(v); } results[0][3] = now_sec() - t0;
    printf("  Done.\n");
    kvs_rb_close(rb);
    
    /* 2. KVS B+Tree */
    printf(">>> KVS (B+Tree)\n");
    KVS_BP *bp = kvs_bp_open();
    t0 = now_sec(); for (int i = 0; i < N; i++) kvs_bp_put(bp, keys[i], vals[i]); results[1][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvs_bp_get(bp, keys[i]); free(v); } results[1][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvs_bp_get(bp, keys[rand()%N]); free(v); } results[1][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = kvs_bp_get(bp, miss[i]); free(v); } results[1][3] = now_sec() - t0;
    printf("  Done.\n");
    kvs_bp_close(bp);
    
    /* 3. Tokyo Cabinet */
    printf(">>> Tokyo Cabinet\n");
    remove("bench.tch");
    TCHDB *tdb = tchdbnew(); tchdbtune(tdb, N*2, -1, -1, HDBTLARGE);
    tchdbopen(tdb, "bench.tch", HDBOWRITER|HDBOCREAT|HDBOTRUNC);
    t0 = now_sec(); for (int i = 0; i < N; i++) tchdbput2(tdb, keys[i], vals[i]); tchdbsync(tdb); results[2][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = tchdbget2(tdb, keys[i]); free(v); } results[2][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = tchdbget2(tdb, keys[rand()%N]); free(v); } results[2][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { char *v = tchdbget2(tdb, miss[i]); free(v); } results[2][3] = now_sec() - t0;
    printf("  Done.\n");
    tchdbclose(tdb); tchdbdel(tdb);
    
    /* 4. Kyoto Cabinet */
    printf(">>> Kyoto Cabinet\n");
    remove("bench.kch");
    kyotocabinet::HashDB kdb; kdb.tune_buckets(N*2);
    kdb.open("bench.kch", kyotocabinet::HashDB::OWRITER|kyotocabinet::HashDB::OCREATE|kyotocabinet::HashDB::OTRUNCATE);
    t0 = now_sec(); for (int i = 0; i < N; i++) kdb.set(keys[i], vals[i]); kdb.synchronize(); results[3][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; kdb.get(keys[i], &v); } results[3][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; kdb.get(keys[rand()%N], &v); } results[3][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; kdb.get(miss[i], &v); } results[3][3] = now_sec() - t0;
    printf("  Done.\n");
    kdb.close();
    
    /* 5. Tkrzw */
    printf(">>> Tkrzw\n");
    remove("bench.tkh");
    tkrzw::HashDBM tkdb; tkrzw::HashDBM::TuningParameters params; params.num_buckets = N*2;
    tkdb.OpenAdvanced("bench.tkh", true, tkrzw::File::OPEN_TRUNCATE, params);
    t0 = now_sec(); for (int i = 0; i < N; i++) tkdb.Set(keys[i], vals[i]); tkdb.Synchronize(false); results[4][0] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; tkdb.Get(keys[i], &v); } results[4][1] = now_sec() - t0;
    srand(12345); t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; tkdb.Get(keys[rand()%N], &v); } results[4][2] = now_sec() - t0;
    t0 = now_sec(); for (int i = 0; i < N; i++) { std::string v; tkdb.Get(miss[i], &v); } results[4][3] = now_sec() - t0;
    printf("  Done.\n");
    tkdb.Close();
    
    /* 6. Berkeley DB */
    printf(">>> Berkeley DB\n");
    remove("bench.bdb");
    DB *bdb; db_create(&bdb, NULL, 0);
    bdb->set_cachesize(bdb, 0, 64*1024*1024, 1);
    bdb->open(bdb, NULL, "bench.bdb", NULL, DB_BTREE, DB_CREATE|DB_TRUNCATE, 0644);
    DBT bkey, bval; memset(&bkey, 0, sizeof(DBT)); memset(&bval, 0, sizeof(DBT));
    t0 = now_sec();
    for (int i = 0; i < N; i++) { bkey.data = keys[i]; bkey.size = strlen(keys[i]); bval.data = vals[i]; bval.size = strlen(vals[i]); bdb->put(bdb, NULL, &bkey, &bval, 0); }
    bdb->sync(bdb, 0); results[5][0] = now_sec() - t0;
    t0 = now_sec();
    for (int i = 0; i < N; i++) { bkey.data = keys[i]; bkey.size = strlen(keys[i]); bval.data = NULL; bval.size = 0; bval.flags = DB_DBT_MALLOC; bdb->get(bdb, NULL, &bkey, &bval, 0); free(bval.data); }
    results[5][1] = now_sec() - t0;
    srand(12345); t0 = now_sec();
    for (int i = 0; i < N; i++) { int idx = rand()%N; bkey.data = keys[idx]; bkey.size = strlen(keys[idx]); bval.data = NULL; bval.size = 0; bval.flags = DB_DBT_MALLOC; bdb->get(bdb, NULL, &bkey, &bval, 0); free(bval.data); }
    results[5][2] = now_sec() - t0;
    t0 = now_sec();
    for (int i = 0; i < N; i++) { bkey.data = miss[i]; bkey.size = strlen(miss[i]); bval.data = NULL; bval.size = 0; bval.flags = DB_DBT_MALLOC; bdb->get(bdb, NULL, &bkey, &bval, 0); }
    results[5][3] = now_sec() - t0;
    printf("  Done.\n");
    bdb->close(bdb, 0);
    
    /* 結果表示 */
    printf("\n╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n");
    printf("║                                           対決結果 (ops/sec)                                                   ║\n");
    printf("╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣\n");
    printf("║ %-10s │ %12s │ %12s │ %12s │ %12s │ %12s │ %12s ║\n", "Operation", names[0], names[1], names[2], names[3], names[4], names[5]);
    printf("╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣\n");
    
    int wins[6] = {0};
    for (int op = 0; op < 4; op++) {
        int best = 0;
        for (int db = 1; db < 6; db++) if (results[db][op] < results[best][op]) best = db;
        wins[best]++;
        printf("║ %-10s │", ops[op]);
        for (int db = 0; db < 6; db++) {
            char buf[20]; sprintf(buf, "%s%.0f", db == best ? "★" : " ", N / results[db][op]);
            printf(" %12s │", buf);
        }
        printf("\n");
    }
    printf("╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝\n");
    
    printf("\n🏆 勝利数:\n");
    for (int i = 0; i < 6; i++) printf("   %s: %d勝\n", names[i], wins[i]);
    
    int winner = 0; for (int i = 1; i < 6; i++) if (wins[i] > wins[winner]) winner = i;
    printf("\n   👑 優勝: %s!\n", names[winner]);
    
    remove("bench.tch"); remove("bench.kch"); remove("bench.tkh"); remove("bench.bdb");
    for (int i = 0; i < N; i++) { free(keys[i]); free(vals[i]); free(miss[i]); }
    free(keys); free(vals); free(miss);
    
    return 0;
}