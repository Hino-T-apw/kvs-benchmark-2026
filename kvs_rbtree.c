/*
 * KVS with Red-Black Tree
 * - Write: リストに追加（O(1)）
 * - Read前にcompact(): リスト→RBTreeに変換
 * - Read: RBTreeから検索（O(log n)）
 * 
 * コンパイル: gcc -O3 -o kvs_rbtree kvs_rbtree.c -lm
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/time.h>

#define POOL_SIZE (128 * 1024 * 1024)
#define RED 0
#define BLACK 1

/* ===== Red-Black Tree Node ===== */
typedef struct RBNode {
    char *key;
    char *value;
    uint32_t klen, vlen;
    int color;
    struct RBNode *left, *right, *parent;
} RBNode;

typedef struct {
    RBNode *root;
    RBNode *nil;  /* 番兵ノード */
} RBTree;

/* ===== Write List Entry ===== */
typedef struct ListEntry {
    char *key;
    char *value;
    uint32_t klen, vlen;
    struct ListEntry *next;
} ListEntry;

/* ===== KVS ===== */
typedef struct {
    /* メモリプール */
    uint8_t *pool;
    size_t pool_pos;
    
    /* 書き込みリスト */
    ListEntry *write_head;
    ListEntry *write_tail;
    size_t write_count;
    
    /* 読み取り用RBTree */
    RBTree *tree;
    int compacted;  /* compact済みフラグ */
    
    size_t total_count;
} KVS;

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
                if (z == z->parent->right) {
                    z = z->parent;
                    rbtree_left_rotate(t, z);
                }
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
                if (z == z->parent->left) {
                    z = z->parent;
                    rbtree_right_rotate(t, z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rbtree_left_rotate(t, z->parent->parent);
            }
        }
    }
    t->root->color = BLACK;
}

void rbtree_insert(RBTree *t, RBNode *z) {
    RBNode *y = t->nil;
    RBNode *x = t->root;
    
    while (x != t->nil) {
        y = x;
        int cmp = strcmp(z->key, x->key);
        if (cmp < 0) x = x->left;
        else if (cmp > 0) x = x->right;
        else {
            /* キー重複：値を更新 */
            x->value = z->value;
            x->vlen = z->vlen;
            free(z);
            return;
        }
    }
    
    z->parent = y;
    if (y == t->nil) t->root = z;
    else if (strcmp(z->key, y->key) < 0) y->left = z;
    else y->right = z;
    
    z->left = t->nil;
    z->right = t->nil;
    z->color = RED;
    rbtree_insert_fixup(t, z);
}

RBNode *rbtree_search(RBTree *t, const char *key) {
    RBNode *x = t->root;
    while (x != t->nil) {
        int cmp = strcmp(key, x->key);
        if (cmp == 0) return x;
        else if (cmp < 0) x = x->left;
        else x = x->right;
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
    rbtree_free_nodes(t, t->root);
    free(t->nil);
    free(t);
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
    db->tree = rbtree_new();
    return db;
}

void kvs_close(KVS *db) {
    if (db) {
        rbtree_free(db->tree);
        munmap(db->pool, POOL_SIZE);
        free(db);
    }
}

/* Write: リストに追加 O(1) */
int kvs_put(KVS *db, const char *key, const char *value) {
    uint32_t klen = strlen(key), vlen = strlen(value);
    
    ListEntry *e = (ListEntry*)pool_alloc(db, sizeof(ListEntry));
    char *k = (char*)pool_alloc(db, klen + 1);
    char *v = (char*)pool_alloc(db, vlen + 1);
    if (!e || !k || !v) return -1;
    
    memcpy(k, key, klen + 1);
    memcpy(v, value, vlen + 1);
    e->key = k;
    e->value = v;
    e->klen = klen;
    e->vlen = vlen;
    e->next = NULL;
    
    if (db->write_tail) {
        db->write_tail->next = e;
        db->write_tail = e;
    } else {
        db->write_head = db->write_tail = e;
    }
    
    db->write_count++;
    db->total_count++;
    db->compacted = 0;  /* 要再compact */
    return 0;
}

/* Compact: リスト→RBTree O(n log n) */
void kvs_compact(KVS *db) {
    if (db->compacted || !db->write_head) return;
    
    ListEntry *e = db->write_head;
    while (e) {
        RBNode *node = (RBNode*)calloc(1, sizeof(RBNode));
        node->key = e->key;
        node->value = e->value;
        node->klen = e->klen;
        node->vlen = e->vlen;
        rbtree_insert(db->tree, node);
        e = e->next;
    }
    
    db->write_head = db->write_tail = NULL;
    db->write_count = 0;
    db->compacted = 1;
}

/* Read: RBTreeから検索 O(log n) */
char *kvs_get(KVS *db, const char *key) {
    /* 未compactならcompact実行 */
    if (!db->compacted) kvs_compact(db);
    
    RBNode *node = rbtree_search(db->tree, key);
    if (node) {
        char *v = (char*)malloc(node->vlen + 1);
        memcpy(v, node->value, node->vlen + 1);
        return v;
    }
    return NULL;
}

/* ===== ベンチマーク ===== */
double now_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

int main(int argc, char **argv) {
    int N = (argc > 1) ? atoi(argv[1]) : 100000;
    
    printf("=== KVS with Red-Black Tree Benchmark ===\n");
    printf("Records: %d\n\n", N);
    
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
    
    /* Write (リスト追加) */
    printf("Write (list append)...\n");
    t0 = now_sec();
    for (int i = 0; i < N; i++) kvs_put(db, keys[i], vals[i]);
    double write_time = now_sec() - t0;
    printf("  %.2f ops/sec (%.4f sec)\n", N / write_time, write_time);
    
    /* Compact (リスト→RBTree) */
    printf("Compact (list -> RBTree)...\n");
    t0 = now_sec();
    kvs_compact(db);
    double compact_time = now_sec() - t0;
    printf("  %.4f sec\n", compact_time);
    
    /* Sequential Read */
    printf("Sequential Read (RBTree)...\n");
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = kvs_get(db, keys[i]);
        free(v);
    }
    double seq_time = now_sec() - t0;
    printf("  %.2f ops/sec (%.4f sec)\n", N / seq_time, seq_time);
    
    /* Random Read */
    printf("Random Read (RBTree)...\n");
    srand(12345);
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = kvs_get(db, keys[rand() % N]);
        free(v);
    }
    double rand_time = now_sec() - t0;
    printf("  %.2f ops/sec (%.4f sec)\n", N / rand_time, rand_time);
    
    /* Miss Read */
    printf("Miss Read (RBTree)...\n");
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        char *v = kvs_get(db, miss[i]);
        free(v);
    }
    double miss_time = now_sec() - t0;
    printf("  %.2f ops/sec (%.4f sec)\n", N / miss_time, miss_time);
    
    printf("\n=== Results ===\n");
    printf("%-15s | %12.2f ops/sec\n", "Write", N / write_time);
    printf("%-15s | %12.4f sec\n", "Compact", compact_time);
    printf("%-15s | %12.2f ops/sec\n", "Seq Read", N / seq_time);
    printf("%-15s | %12.2f ops/sec\n", "Rand Read", N / rand_time);
    printf("%-15s | %12.2f ops/sec\n", "Miss Read", N / miss_time);
    
    printf("\nMemory: %.2f MB\n", db->pool_pos / (1024.0 * 1024.0));
    
    /* 検証 */
    printf("\n=== Verification ===\n");
    char *v = kvs_get(db, keys[0]);
    printf("%s = %s\n", keys[0], v ? v : "(null)");
    free(v);
    v = kvs_get(db, keys[N-1]);
    printf("%s = %s\n", keys[N-1], v ? v : "(null)");
    free(v);
    v = kvs_get(db, miss[0]);
    printf("%s = %s\n", miss[0], v ? v : "(null)");
    free(v);
    
    kvs_close(db);
    for (int i = 0; i < N; i++) { free(keys[i]); free(vals[i]); free(miss[i]); }
    free(keys); free(vals); free(miss);
    
    return 0;
}