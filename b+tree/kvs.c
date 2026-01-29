/*
 * KVS - B+Tree Implementation
 */
#include "kvs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define ORDER KVS_BPTREE_ORDER
#define MIN_KEYS ((ORDER - 1) / 2)
#define BLOOM_EXPAND_THRESHOLD 0.5

/* ===== Entry ===== */
typedef struct {
    char *key;
    char *value;
    uint32_t klen, vlen;
    uint8_t deleted;
} Entry;

/* ===== B+Tree Node ===== */
typedef struct BPNode {
    int is_leaf;
    int num_keys;
    char *keys[ORDER - 1];        /* キーへのポインタ */
    uint32_t klens[ORDER - 1];    /* キー長 */
    union {
        struct BPNode *children[ORDER];  /* 内部ノード: 子ノード */
        Entry *entries[ORDER - 1];       /* リーフ: エントリ */
    };
    struct BPNode *next;  /* リーフノード間リンク */
    struct BPNode *prev;
} BPNode;

/* ===== KVS ===== */
struct KVS {
    uint8_t *pool;
    size_t pool_size;
    size_t pool_pos;
    
    BPNode *root;
    BPNode *first_leaf;  /* 最左リーフ */
    size_t count;
    size_t height;
    size_t node_count;
    
    uint8_t *bloom;
    size_t bloom_bits;
    size_t bloom_set_bits;
    
    char *filepath;
};

/* ===== Cursor ===== */
struct KVSCursor {
    KVS *db;
    BPNode *node;
    int index;
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

static void bloom_rebuild(KVS *db);

static void bloom_expand(KVS *db) {
    size_t new_bits = db->bloom_bits * 4;
    if (new_bits > KVS_BLOOM_MAX_BITS) new_bits = KVS_BLOOM_MAX_BITS;
    if (new_bits == db->bloom_bits) return;
    
    free(db->bloom);
    db->bloom = (uint8_t*)calloc(new_bits / 8, 1);
    db->bloom_bits = new_bits;
    db->bloom_set_bits = 0;
    bloom_rebuild(db);
}

static void bloom_check_expand(KVS *db) {
    double fill = (double)db->bloom_set_bits / db->bloom_bits;
    if (fill >= BLOOM_EXPAND_THRESHOLD && db->bloom_bits < KVS_BLOOM_MAX_BITS)
        bloom_expand(db);
}

/* ===== Key Comparison ===== */
static inline int keycmp(const char *k1, size_t l1, const char *k2, size_t l2) {
    size_t min = l1 < l2 ? l1 : l2;
    int cmp = memcmp(k1, k2, min);
    if (cmp != 0) return cmp;
    return (l1 < l2) ? -1 : (l1 > l2) ? 1 : 0;
}

/* ===== Memory Pool ===== */
static void *pool_alloc(KVS *db, size_t size) {
    size = (size + 7) & ~7;
    if (db->pool_pos + size > db->pool_size) return NULL;
    void *p = db->pool + db->pool_pos;
    db->pool_pos += size;
    return p;
}

/* ===== B+Tree Operations ===== */
static BPNode *bpnode_new(KVS *db, int is_leaf) {
    BPNode *n = (BPNode*)calloc(1, sizeof(BPNode));
    n->is_leaf = is_leaf;
    db->node_count++;
    return n;
}

/* リーフノードでキーを検索（挿入位置を返す） */
static int leaf_find_pos(BPNode *leaf, const char *key, size_t klen) {
    int lo = 0, hi = leaf->num_keys;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (keycmp(leaf->keys[mid], leaf->klens[mid], key, klen) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

/* 内部ノードで子を検索 */
static int internal_find_pos(BPNode *node, const char *key, size_t klen) {
    int lo = 0, hi = node->num_keys;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (keycmp(node->keys[mid], node->klens[mid], key, klen) <= 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

/* ルートから適切なリーフを探す */
static BPNode *find_leaf(KVS *db, const char *key, size_t klen) {
    if (!db->root) return NULL;
    BPNode *n = db->root;
    while (!n->is_leaf) {
        int pos = internal_find_pos(n, key, klen);
        n = n->children[pos];
    }
    return n;
}

/* リーフノードを分割 */
static void split_leaf(KVS *db, BPNode *leaf, BPNode *parent, int parent_pos) {
    BPNode *new_leaf = bpnode_new(db, 1);
    int mid = leaf->num_keys / 2;
    
    /* 後半をnew_leafに移動 */
    new_leaf->num_keys = leaf->num_keys - mid;
    for (int i = 0; i < new_leaf->num_keys; i++) {
        new_leaf->keys[i] = leaf->keys[mid + i];
        new_leaf->klens[i] = leaf->klens[mid + i];
        new_leaf->entries[i] = leaf->entries[mid + i];
    }
    leaf->num_keys = mid;
    
    /* リーフリンク更新 */
    new_leaf->next = leaf->next;
    new_leaf->prev = leaf;
    if (leaf->next) leaf->next->prev = new_leaf;
    leaf->next = new_leaf;
    
    /* 親に挿入するキー */
    char *up_key = new_leaf->keys[0];
    uint32_t up_klen = new_leaf->klens[0];
    
    if (!parent) {
        /* 新しいルート */
        BPNode *new_root = bpnode_new(db, 0);
        new_root->num_keys = 1;
        new_root->keys[0] = up_key;
        new_root->klens[0] = up_klen;
        new_root->children[0] = leaf;
        new_root->children[1] = new_leaf;
        db->root = new_root;
        db->height++;
    } else {
        /* 親に挿入 */
        for (int i = parent->num_keys; i > parent_pos; i--) {
            parent->keys[i] = parent->keys[i - 1];
            parent->klens[i] = parent->klens[i - 1];
            parent->children[i + 1] = parent->children[i];
        }
        parent->keys[parent_pos] = up_key;
        parent->klens[parent_pos] = up_klen;
        parent->children[parent_pos + 1] = new_leaf;
        parent->num_keys++;
    }
}

/* 内部ノードを分割 */
static void split_internal(KVS *db, BPNode *node, BPNode *parent, int parent_pos) {
    BPNode *new_node = bpnode_new(db, 0);
    int mid = node->num_keys / 2;
    
    char *up_key = node->keys[mid];
    uint32_t up_klen = node->klens[mid];
    
    new_node->num_keys = node->num_keys - mid - 1;
    for (int i = 0; i < new_node->num_keys; i++) {
        new_node->keys[i] = node->keys[mid + 1 + i];
        new_node->klens[i] = node->klens[mid + 1 + i];
        new_node->children[i] = node->children[mid + 1 + i];
    }
    new_node->children[new_node->num_keys] = node->children[node->num_keys];
    node->num_keys = mid;
    
    if (!parent) {
        BPNode *new_root = bpnode_new(db, 0);
        new_root->num_keys = 1;
        new_root->keys[0] = up_key;
        new_root->klens[0] = up_klen;
        new_root->children[0] = node;
        new_root->children[1] = new_node;
        db->root = new_root;
        db->height++;
    } else {
        for (int i = parent->num_keys; i > parent_pos; i--) {
            parent->keys[i] = parent->keys[i - 1];
            parent->klens[i] = parent->klens[i - 1];
            parent->children[i + 1] = parent->children[i];
        }
        parent->keys[parent_pos] = up_key;
        parent->klens[parent_pos] = up_klen;
        parent->children[parent_pos + 1] = new_node;
        parent->num_keys++;
    }
}

/* 挿入（再帰） */
static int insert_recursive(KVS *db, BPNode *node, BPNode *parent, int parent_pos,
                           const char *key, size_t klen, Entry *entry) {
    if (node->is_leaf) {
        int pos = leaf_find_pos(node, key, klen);
        
        /* 既存キーの更新チェック */
        if (pos < node->num_keys && 
            keycmp(node->keys[pos], node->klens[pos], key, klen) == 0) {
            if (!node->entries[pos]->deleted) {
                node->entries[pos]->value = entry->value;
                node->entries[pos]->vlen = entry->vlen;
                return 0;  /* 更新のみ、カウント増加なし */
            }
            node->entries[pos] = entry;
            return 1;
        }
        
        /* 新規挿入 */
        for (int i = node->num_keys; i > pos; i--) {
            node->keys[i] = node->keys[i - 1];
            node->klens[i] = node->klens[i - 1];
            node->entries[i] = node->entries[i - 1];
        }
        node->keys[pos] = entry->key;
        node->klens[pos] = klen;
        node->entries[pos] = entry;
        node->num_keys++;
        
        /* 分割が必要か */
        if (node->num_keys >= ORDER - 1) {
            split_leaf(db, node, parent, parent_pos);
        }
        return 1;
    } else {
        int pos = internal_find_pos(node, key, klen);
        int added = insert_recursive(db, node->children[pos], node, pos, key, klen, entry);
        
        if (node->num_keys >= ORDER - 1) {
            split_internal(db, node, parent, parent_pos);
        }
        return added;
    }
}

/* ===== Bloom Rebuild ===== */
static void bloom_rebuild(KVS *db) {
    BPNode *leaf = db->first_leaf;
    while (leaf) {
        for (int i = 0; i < leaf->num_keys; i++) {
            if (!leaf->entries[i]->deleted) {
                bloom_add(db, leaf->keys[i], leaf->klens[i]);
            }
        }
        leaf = leaf->next;
    }
}

/* ===== Public API ===== */
KVS *kvs_open(const char *path) {
    KVS *db = (KVS*)calloc(1, sizeof(KVS));
    if (!db) return NULL;
    
    db->pool_size = KVS_DEFAULT_POOL_SIZE;
    db->pool = (uint8_t*)mmap(NULL, db->pool_size, PROT_READ|PROT_WRITE,
                              MAP_PRIVATE|MAP_ANON, -1, 0);
    if (db->pool == MAP_FAILED) db->pool = (uint8_t*)malloc(db->pool_size);
    if (!db->pool) { free(db); return NULL; }
    
    db->bloom_bits = KVS_BLOOM_INIT_BITS;
    db->bloom = (uint8_t*)calloc(db->bloom_bits / 8, 1);
    
    /* 空のリーフをルートとして作成 */
    db->root = bpnode_new(db, 1);
    db->first_leaf = db->root;
    db->height = 1;
    
    if (path) db->filepath = strdup(path);
    
    return db;
}

static void free_node(BPNode *n) {
    if (!n) return;
    if (!n->is_leaf) {
        for (int i = 0; i <= n->num_keys; i++)
            free_node(n->children[i]);
    }
    free(n);
}

void kvs_close(KVS *db) {
    if (!db) return;
    if (db->filepath) {
        kvs_save(db, db->filepath);
        free(db->filepath);
    }
    free_node(db->root);
    free(db->bloom);
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
    e->deleted = 0;
    
    bloom_add(db, key, klen);
    
    int added = insert_recursive(db, db->root, NULL, 0, key, klen, e);
    if (added) db->count++;
    
    /* first_leaf更新 */
    BPNode *n = db->root;
    while (!n->is_leaf) n = n->children[0];
    db->first_leaf = n;
    
    if (db->count % 1000 == 0) bloom_check_expand(db);
    
    return KVS_OK;
}

int kvs_put(KVS *db, const char *key, const char *value) {
    return kvs_put_raw(db, key, strlen(key), value, strlen(value));
}

char *kvs_get_raw(KVS *db, const char *key, size_t klen, size_t *vlen) {
    if (!bloom_maybe(db, key, klen)) return NULL;
    
    BPNode *leaf = find_leaf(db, key, klen);
    if (!leaf) return NULL;
    
    int pos = leaf_find_pos(leaf, key, klen);
    if (pos < leaf->num_keys &&
        keycmp(leaf->keys[pos], leaf->klens[pos], key, klen) == 0 &&
        !leaf->entries[pos]->deleted) {
        Entry *e = leaf->entries[pos];
        char *v = (char*)malloc(e->vlen + 1);
        memcpy(v, e->value, e->vlen + 1);
        if (vlen) *vlen = e->vlen;
        return v;
    }
    return NULL;
}

char *kvs_get(KVS *db, const char *key) {
    return kvs_get_raw(db, key, strlen(key), NULL);
}

int kvs_exists(KVS *db, const char *key) {
    size_t klen = strlen(key);
    if (!bloom_maybe(db, key, klen)) return 0;
    
    BPNode *leaf = find_leaf(db, key, klen);
    if (!leaf) return 0;
    
    int pos = leaf_find_pos(leaf, key, klen);
    return (pos < leaf->num_keys &&
            keycmp(leaf->keys[pos], leaf->klens[pos], key, klen) == 0 &&
            !leaf->entries[pos]->deleted);
}

int kvs_delete(KVS *db, const char *key) {
    size_t klen = strlen(key);
    if (!bloom_maybe(db, key, klen)) return KVS_ERR_NOTFOUND;
    
    BPNode *leaf = find_leaf(db, key, klen);
    if (!leaf) return KVS_ERR_NOTFOUND;
    
    int pos = leaf_find_pos(leaf, key, klen);
    if (pos < leaf->num_keys &&
        keycmp(leaf->keys[pos], leaf->klens[pos], key, klen) == 0 &&
        !leaf->entries[pos]->deleted) {
        leaf->entries[pos]->deleted = 1;
        db->count--;
        return KVS_OK;
    }
    return KVS_ERR_NOTFOUND;
}

/* ===== Cursor ===== */
KVSCursor *kvs_cursor_new(KVS *db) {
    KVSCursor *cur = (KVSCursor*)calloc(1, sizeof(KVSCursor));
    cur->db = db;
    return cur;
}

void kvs_cursor_free(KVSCursor *cur) { free(cur); }

int kvs_cursor_first(KVSCursor *cur) {
    cur->node = cur->db->first_leaf;
    cur->index = 0;
    while (cur->node && cur->index < cur->node->num_keys &&
           cur->node->entries[cur->index]->deleted) {
        kvs_cursor_next(cur);
    }
    return kvs_cursor_valid(cur);
}

int kvs_cursor_last(KVSCursor *cur) {
    BPNode *n = cur->db->root;
    while (!n->is_leaf) n = n->children[n->num_keys];
    cur->node = n;
    cur->index = n->num_keys - 1;
    while (cur->index >= 0 && cur->node->entries[cur->index]->deleted) {
        kvs_cursor_prev(cur);
    }
    return kvs_cursor_valid(cur);
}

int kvs_cursor_seek(KVSCursor *cur, const char *key) {
    size_t klen = strlen(key);
    cur->node = find_leaf(cur->db, key, klen);
    if (!cur->node) return 0;
    cur->index = leaf_find_pos(cur->node, key, klen);
    if (cur->index >= cur->node->num_keys) {
        cur->node = cur->node->next;
        cur->index = 0;
    }
    return kvs_cursor_valid(cur);
}

int kvs_cursor_next(KVSCursor *cur) {
    if (!cur->node) return 0;
    do {
        cur->index++;
        if (cur->index >= cur->node->num_keys) {
            cur->node = cur->node->next;
            cur->index = 0;
        }
    } while (cur->node && cur->index < cur->node->num_keys &&
             cur->node->entries[cur->index]->deleted);
    return kvs_cursor_valid(cur);
}

int kvs_cursor_prev(KVSCursor *cur) {
    if (!cur->node) return 0;
    do {
        cur->index--;
        if (cur->index < 0) {
            cur->node = cur->node->prev;
            if (cur->node) cur->index = cur->node->num_keys - 1;
        }
    } while (cur->node && cur->index >= 0 &&
             cur->node->entries[cur->index]->deleted);
    return kvs_cursor_valid(cur);
}

int kvs_cursor_valid(KVSCursor *cur) {
    return cur->node && cur->index >= 0 && cur->index < cur->node->num_keys;
}

const char *kvs_cursor_key(KVSCursor *cur, size_t *klen) {
    if (!kvs_cursor_valid(cur)) return NULL;
    if (klen) *klen = cur->node->klens[cur->index];
    return cur->node->keys[cur->index];
}

const char *kvs_cursor_value(KVSCursor *cur, size_t *vlen) {
    if (!kvs_cursor_valid(cur)) return NULL;
    Entry *e = cur->node->entries[cur->index];
    if (vlen) *vlen = e->vlen;
    return e->value;
}

/* ===== Persistence ===== */
#define KVS_MAGIC 0x54504253  /* "SBPT" */

int kvs_save(KVS *db, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) return KVS_ERR_IO;
    
    uint32_t magic = KVS_MAGIC;
    fwrite(&magic, 4, 1, f);
    fwrite(&db->count, sizeof(size_t), 1, f);
    fwrite(&db->bloom_bits, sizeof(size_t), 1, f);
    fwrite(db->bloom, db->bloom_bits / 8, 1, f);
    
    /* 全エントリを順次書き出し */
    BPNode *leaf = db->first_leaf;
    while (leaf) {
        for (int i = 0; i < leaf->num_keys; i++) {
            Entry *e = leaf->entries[i];
            if (!e->deleted) {
                fwrite(&e->klen, 4, 1, f);
                fwrite(&e->vlen, 4, 1, f);
                fwrite(e->key, e->klen, 1, f);
                fwrite(e->value, e->vlen, 1, f);
            }
        }
        leaf = leaf->next;
    }
    
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
    size_t count;
    fread(&count, sizeof(size_t), 1, f);
    fread(&db->bloom_bits, sizeof(size_t), 1, f);
    
    free(db->bloom);
    db->bloom = (uint8_t*)malloc(db->bloom_bits / 8);
    fread(db->bloom, db->bloom_bits / 8, 1, f);
    
    for (size_t i = 0; i < count; i++) {
        uint32_t klen, vlen;
        fread(&klen, 4, 1, f);
        fread(&vlen, 4, 1, f);
        char *key = (char*)malloc(klen + 1);
        char *val = (char*)malloc(vlen + 1);
        fread(key, klen, 1, f);
        fread(val, vlen, 1, f);
        key[klen] = val[vlen] = '\0';
        kvs_put_raw(db, key, klen, val, vlen);
        free(key); free(val);
    }
    
    fclose(f);
    return db;
}

/* ===== Utilities ===== */
void kvs_stats(KVS *db, KVSStats *stats) {
    stats->count = db->count;
    stats->memory_used = db->pool_pos;
    stats->bloom_bits = db->bloom_bits;
    stats->bloom_fill_rate = (double)db->bloom_set_bits / db->bloom_bits * 100.0;
    stats->tree_height = db->height;
    stats->node_count = db->node_count;
}

size_t kvs_foreach(KVS *db, kvs_iter_fn callback, void *userdata) {
    size_t count = 0;
    BPNode *leaf = db->first_leaf;
    while (leaf) {
        for (int i = 0; i < leaf->num_keys; i++) {
            Entry *e = leaf->entries[i];
            if (!e->deleted) {
                callback(e->key, e->klen, e->value, e->vlen, userdata);
                count++;
            }
        }
        leaf = leaf->next;
    }
    return count;
}

size_t kvs_range(KVS *db, const char *from, const char *to,
                 kvs_iter_fn callback, void *userdata) {
    size_t count = 0;
    size_t from_len = strlen(from);
    size_t to_len = strlen(to);
    
    KVSCursor *cur = kvs_cursor_new(db);
    kvs_cursor_seek(cur, from);
    
    while (kvs_cursor_valid(cur)) {
        size_t klen;
        const char *key = kvs_cursor_key(cur, &klen);
        if (keycmp(key, klen, to, to_len) > 0) break;
        
        size_t vlen;
        const char *val = kvs_cursor_value(cur, &vlen);
        callback(key, klen, val, vlen, userdata);
        count++;
        
        kvs_cursor_next(cur);
    }
    
    kvs_cursor_free(cur);
    return count;
}