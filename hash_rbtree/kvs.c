/*
 * KVS - High Performance Key-Value Store Library
 * Implementation
 */
#include "kvs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define RED 0
#define BLACK 1
#define BLOOM_EXPAND_THRESHOLD 0.5

/* ===== Internal Structures ===== */
typedef struct Entry {
    char *key;
    char *value;
    uint32_t klen, vlen;
    uint8_t deleted;
    struct Entry *hash_next;
} Entry;

typedef struct RBNode {
    Entry *entry;
    int color;
    struct RBNode *left, *right, *parent;
} RBNode;

typedef struct RBTree {
    RBNode *root;
    RBNode *nil;
} RBTree;

struct KVS {
    uint8_t *pool;
    size_t pool_size;
    size_t pool_pos;
    
    KVSMode mode;
    size_t count;
    
    Entry **buckets;
    size_t bucket_count;
    size_t threshold;
    
    uint8_t *bloom;
    size_t bloom_bits;
    size_t bloom_set_bits;
    
    RBTree *tree;
    char *filepath;
};

/* ===== Hash Functions ===== */
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

/* ===== Bloom Filter ===== */
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

static void bloom_add(KVS *db, const char *k, size_t l) {
    size_t bits = db->bloom_bits;
    bloom_set_bit(db, fnv1a(k, l) % bits);
    bloom_set_bit(db, hash2(k, l) % bits);
    bloom_set_bit(db, hash3(k, l) % bits);
}

static int bloom_maybe(KVS *db, const char *k, size_t l) {
    size_t bits = db->bloom_bits;
    return bloom_get_bit(db, fnv1a(k, l) % bits) &&
           bloom_get_bit(db, hash2(k, l) % bits) &&
           bloom_get_bit(db, hash3(k, l) % bits);
}

/* Forward declarations */
static void rbtree_inorder(KVS *db, RBNode *n, void (*fn)(Entry*, void*), void *arg);

static void bloom_rehash_entry(Entry *e, void *arg) {
    KVS *db = (KVS*)arg;
    if (!e->deleted) bloom_add(db, e->key, e->klen);
}

static void bloom_expand(KVS *db) {
    size_t new_bits = db->bloom_bits * 4;
    if (new_bits > KVS_BLOOM_MAX_BITS) new_bits = KVS_BLOOM_MAX_BITS;
    if (new_bits == db->bloom_bits) return;
    
    free(db->bloom);
    db->bloom = (uint8_t*)calloc(new_bits / 8, 1);
    db->bloom_bits = new_bits;
    db->bloom_set_bits = 0;
    
    if (db->mode == KVS_MODE_HASH) {
        for (size_t i = 0; i < db->bucket_count; i++) {
            Entry *e = db->buckets[i];
            while (e) { bloom_rehash_entry(e, db); e = e->hash_next; }
        }
    } else {
        rbtree_inorder(db, db->tree->root, bloom_rehash_entry, db);
    }
}

static void bloom_check_expand(KVS *db) {
    double fill = (double)db->bloom_set_bits / db->bloom_bits;
    if (fill >= BLOOM_EXPAND_THRESHOLD && db->bloom_bits < KVS_BLOOM_MAX_BITS)
        bloom_expand(db);
}

/* ===== RBTree ===== */
static RBTree *rbtree_new(void) {
    RBTree *t = (RBTree*)calloc(1, sizeof(RBTree));
    t->nil = (RBNode*)calloc(1, sizeof(RBNode));
    t->nil->color = BLACK;
    t->root = t->nil;
    return t;
}

static void rbtree_left_rotate(RBTree *t, RBNode *x) {
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

static void rbtree_right_rotate(RBTree *t, RBNode *x) {
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

static void rbtree_insert_fixup(RBTree *t, RBNode *z) {
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

static int keycmp(const char *k1, size_t l1, const char *k2, size_t l2) {
    size_t min = l1 < l2 ? l1 : l2;
    int cmp = memcmp(k1, k2, min);
    if (cmp != 0) return cmp;
    return (l1 < l2) ? -1 : (l1 > l2) ? 1 : 0;
}

static void rbtree_insert(RBTree *t, RBNode *z) {
    RBNode *y = t->nil, *x = t->root;
    while (x != t->nil) {
        y = x;
        int cmp = keycmp(z->entry->key, z->entry->klen, x->entry->key, x->entry->klen);
        if (cmp < 0) x = x->left;
        else if (cmp > 0) x = x->right;
        else { x->entry = z->entry; free(z); return; }
    }
    z->parent = y;
    if (y == t->nil) t->root = z;
    else if (keycmp(z->entry->key, z->entry->klen, y->entry->key, y->entry->klen) < 0) y->left = z;
    else y->right = z;
    z->left = z->right = t->nil;
    z->color = RED;
    rbtree_insert_fixup(t, z);
}

static RBNode *rbtree_search(RBTree *t, const char *key, size_t klen) {
    RBNode *x = t->root;
    while (x != t->nil) {
        int cmp = keycmp(key, klen, x->entry->key, x->entry->klen);
        if (cmp == 0) return x;
        x = (cmp < 0) ? x->left : x->right;
    }
    return NULL;
}

static void rbtree_inorder(KVS *db, RBNode *n, void (*fn)(Entry*, void*), void *arg) {
    if (n != db->tree->nil) {
        rbtree_inorder(db, n->left, fn, arg);
        fn(n->entry, arg);
        rbtree_inorder(db, n->right, fn, arg);
    }
}

static void rbtree_free_nodes(RBTree *t, RBNode *n) {
    if (n != t->nil) {
        rbtree_free_nodes(t, n->left);
        rbtree_free_nodes(t, n->right);
        free(n);
    }
}

static void rbtree_free(RBTree *t) {
    if (t) { rbtree_free_nodes(t, t->root); free(t->nil); free(t); }
}

/* ===== Memory Pool ===== */
static void *pool_alloc(KVS *db, size_t size) {
    size = (size + 7) & ~7;
    if (db->pool_pos + size > db->pool_size) return NULL;
    void *p = db->pool + db->pool_pos;
    db->pool_pos += size;
    return p;
}

/* ===== Mode Conversion ===== */
static void kvs_convert_to_rbtree(KVS *db) {
    for (size_t i = 0; i < db->bucket_count; i++) {
        Entry *e = db->buckets[i];
        while (e) {
            if (!e->deleted) {
                RBNode *node = (RBNode*)calloc(1, sizeof(RBNode));
                node->entry = e;
                rbtree_insert(db->tree, node);
            }
            e = e->hash_next;
        }
        db->buckets[i] = NULL;
    }
    db->mode = KVS_MODE_RBTREE;
}

/* ===== Public API ===== */
KVS *kvs_open(const char *path) {
    KVS *db = (KVS*)calloc(1, sizeof(KVS));
    if (!db) return NULL;
    
    db->pool_size = KVS_DEFAULT_POOL_SIZE;
    db->pool = (uint8_t*)mmap(NULL, db->pool_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
    if (db->pool == MAP_FAILED) db->pool = (uint8_t*)malloc(db->pool_size);
    if (!db->pool) { free(db); return NULL; }
    
    db->bucket_count = KVS_DEFAULT_HASH_BUCKETS;
    db->threshold = KVS_DEFAULT_THRESHOLD;
    db->buckets = (Entry**)calloc(db->bucket_count, sizeof(Entry*));
    
    db->bloom_bits = KVS_BLOOM_INIT_BITS;
    db->bloom = (uint8_t*)calloc(db->bloom_bits / 8, 1);
    
    db->tree = rbtree_new();
    db->mode = KVS_MODE_HASH;
    
    if (path) db->filepath = strdup(path);
    
    return db;
}

void kvs_close(KVS *db) {
    if (!db) return;
    if (db->filepath) {
        kvs_save(db, db->filepath);
        free(db->filepath);
    }
    free(db->buckets);
    free(db->bloom);
    rbtree_free(db->tree);
    munmap(db->pool, db->pool_size);
    free(db);
}

int kvs_put_raw(KVS *db, const char *key, size_t klen, const char *value, size_t vlen) {
    Entry *e = (Entry*)pool_alloc(db, sizeof(Entry));
    char *k = (char*)pool_alloc(db, klen + 1);
    char *v = (char*)pool_alloc(db, vlen + 1);
    if (!e || !k || !v) return KVS_ERR_NOMEM;
    
    memcpy(k, key, klen); k[klen] = '\0';
    memcpy(v, value, vlen); v[vlen] = '\0';
    e->key = k; e->value = v;
    e->klen = klen; e->vlen = vlen;
    e->deleted = 0; e->hash_next = NULL;
    
    bloom_add(db, key, klen);
    
    if (db->mode == KVS_MODE_HASH) {
        uint32_t bucket = fnv1a(key, klen) % db->bucket_count;
        
        /* Check for existing key */
        Entry *cur = db->buckets[bucket];
        while (cur) {
            if (cur->klen == klen && memcmp(cur->key, key, klen) == 0 && !cur->deleted) {
                cur->value = v; cur->vlen = vlen;
                return KVS_OK;
            }
            cur = cur->hash_next;
        }
        
        e->hash_next = db->buckets[bucket];
        db->buckets[bucket] = e;
        db->count++;
        
        if (db->count >= db->threshold) kvs_convert_to_rbtree(db);
    } else {
        RBNode *node = (RBNode*)calloc(1, sizeof(RBNode));
        node->entry = e;
        rbtree_insert(db->tree, node);
        db->count++;
    }
    
    if (db->count % 1000 == 0) bloom_check_expand(db);
    return KVS_OK;
}

int kvs_put(KVS *db, const char *key, const char *value) {
    return kvs_put_raw(db, key, strlen(key), value, strlen(value));
}

char *kvs_get_raw(KVS *db, const char *key, size_t klen, size_t *vlen) {
    if (!bloom_maybe(db, key, klen)) return NULL;
    
    Entry *e = NULL;
    if (db->mode == KVS_MODE_HASH) {
        uint32_t bucket = fnv1a(key, klen) % db->bucket_count;
        e = db->buckets[bucket];
        while (e) {
            if (e->klen == klen && memcmp(e->key, key, klen) == 0 && !e->deleted) break;
            e = e->hash_next;
        }
    } else {
        RBNode *node = rbtree_search(db->tree, key, klen);
        if (node && !node->entry->deleted) e = node->entry;
    }
    
    if (!e) return NULL;
    
    char *v = (char*)malloc(e->vlen + 1);
    memcpy(v, e->value, e->vlen + 1);
    if (vlen) *vlen = e->vlen;
    return v;
}

char *kvs_get(KVS *db, const char *key) {
    return kvs_get_raw(db, key, strlen(key), NULL);
}

int kvs_exists(KVS *db, const char *key) {
    size_t klen = strlen(key);
    if (!bloom_maybe(db, key, klen)) return 0;
    
    if (db->mode == KVS_MODE_HASH) {
        Entry *e = db->buckets[fnv1a(key, klen) % db->bucket_count];
        while (e) {
            if (e->klen == klen && memcmp(e->key, key, klen) == 0 && !e->deleted) return 1;
            e = e->hash_next;
        }
    } else {
        RBNode *node = rbtree_search(db->tree, key, klen);
        if (node && !node->entry->deleted) return 1;
    }
    return 0;
}

int kvs_delete(KVS *db, const char *key) {
    size_t klen = strlen(key);
    if (!bloom_maybe(db, key, klen)) return KVS_ERR_NOTFOUND;
    
    if (db->mode == KVS_MODE_HASH) {
        Entry *e = db->buckets[fnv1a(key, klen) % db->bucket_count];
        while (e) {
            if (e->klen == klen && memcmp(e->key, key, klen) == 0 && !e->deleted) {
                e->deleted = 1;
                db->count--;
                return KVS_OK;
            }
            e = e->hash_next;
        }
    } else {
        RBNode *node = rbtree_search(db->tree, key, klen);
        if (node && !node->entry->deleted) {
            node->entry->deleted = 1;
            db->count--;
            return KVS_OK;
        }
    }
    return KVS_ERR_NOTFOUND;
}

/* ===== Persistence ===== */
#define KVS_MAGIC 0x5356534B  /* "KSVS" */

int kvs_save(KVS *db, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) return KVS_ERR_IO;
    
    uint32_t magic = KVS_MAGIC;
    fwrite(&magic, 4, 1, f);
    fwrite(&db->pool_pos, sizeof(size_t), 1, f);
    fwrite(&db->count, sizeof(size_t), 1, f);
    fwrite(&db->mode, sizeof(KVSMode), 1, f);
    fwrite(&db->bloom_bits, sizeof(size_t), 1, f);
    fwrite(&db->bloom_set_bits, sizeof(size_t), 1, f);
    fwrite(db->bloom, db->bloom_bits / 8, 1, f);
    fwrite(db->pool, db->pool_pos, 1, f);
    
    fclose(f);
    return KVS_OK;
}

KVS *kvs_load(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    
    uint32_t magic;
    fread(&magic, 4, 1, f);
    if (magic != KVS_MAGIC) { fclose(f); return NULL; }
    
    KVS *db = kvs_open(NULL);
    if (!db) { fclose(f); return NULL; }
    
    fread(&db->pool_pos, sizeof(size_t), 1, f);
    fread(&db->count, sizeof(size_t), 1, f);
    fread(&db->mode, sizeof(KVSMode), 1, f);
    fread(&db->bloom_bits, sizeof(size_t), 1, f);
    fread(&db->bloom_set_bits, sizeof(size_t), 1, f);
    
    free(db->bloom);
    db->bloom = (uint8_t*)malloc(db->bloom_bits / 8);
    fread(db->bloom, db->bloom_bits / 8, 1, f);
    fread(db->pool, db->pool_pos, 1, f);
    
    /* Rebuild index */
    /* TODO: Save/restore hash buckets or RBTree structure */
    
    fclose(f);
    return db;
}

/* ===== Utilities ===== */
void kvs_stats(KVS *db, KVSStats *stats) {
    stats->count = db->count;
    stats->memory_used = db->pool_pos;
    stats->bloom_bits = db->bloom_bits;
    stats->bloom_fill_rate = (double)db->bloom_set_bits / db->bloom_bits * 100.0;
    stats->mode = db->mode;
}

const char *kvs_mode_str(KVS *db) {
    return db->mode == KVS_MODE_HASH ? "Hash" : "RBTree";
}

void kvs_compact(KVS *db) {
    if (db->mode == KVS_MODE_HASH) kvs_convert_to_rbtree(db);
}

typedef struct { kvs_iter_fn fn; void *arg; } IterCtx;

static void iter_entry(Entry *e, void *arg) {
    IterCtx *ctx = (IterCtx*)arg;
    if (!e->deleted) ctx->fn(e->key, e->klen, e->value, e->vlen, ctx->arg);
}

size_t kvs_foreach(KVS *db, kvs_iter_fn callback, void *userdata) {
    IterCtx ctx = { callback, userdata };
    size_t count = 0;
    
    if (db->mode == KVS_MODE_HASH) {
        for (size_t i = 0; i < db->bucket_count; i++) {
            Entry *e = db->buckets[i];
            while (e) {
                if (!e->deleted) { callback(e->key, e->klen, e->value, e->vlen, userdata); count++; }
                e = e->hash_next;
            }
        }
    } else {
        rbtree_inorder(db, db->tree->root, iter_entry, &ctx);
        count = db->count;
    }
    return count;
}