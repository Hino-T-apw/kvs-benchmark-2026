/*
 * KVS - High Performance Key-Value Store Library (B+Tree Edition)
 * 
 * Features:
 *   - B+Tree index (cache-friendly, O(log n))
 *   - Dynamic Bloom filter
 *   - Memory pool allocation
 *   - File persistence
 *   - Range queries support
 * 
 * License: MIT
 */
#ifndef KVS_H
#define KVS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ===== Configuration ===== */
#define KVS_DEFAULT_POOL_SIZE    (128 * 1024 * 1024)
#define KVS_BPTREE_ORDER         64    /* ノードあたりの最大キー数 */
#define KVS_BLOOM_INIT_BITS      (1 << 20)
#define KVS_BLOOM_MAX_BITS       (1 << 26)

/* ===== Types ===== */
typedef struct KVS KVS;

typedef enum {
    KVS_OK = 0,
    KVS_ERR_NOMEM = -1,
    KVS_ERR_NOTFOUND = -2,
    KVS_ERR_IO = -3,
    KVS_ERR_FULL = -4
} KVSError;

typedef struct {
    size_t count;
    size_t memory_used;
    size_t bloom_bits;
    double bloom_fill_rate;
    size_t tree_height;
    size_t node_count;
} KVSStats;

/* ===== Core API ===== */
KVS *kvs_open(const char *path);
void kvs_close(KVS *db);

int kvs_put(KVS *db, const char *key, const char *value);
int kvs_put_raw(KVS *db, const char *key, size_t klen, const char *value, size_t vlen);

char *kvs_get(KVS *db, const char *key);
char *kvs_get_raw(KVS *db, const char *key, size_t klen, size_t *vlen);

int kvs_exists(KVS *db, const char *key);
int kvs_delete(KVS *db, const char *key);

/* ===== Range Query (B+Tree特有) ===== */
typedef struct KVSCursor KVSCursor;

KVSCursor *kvs_cursor_new(KVS *db);
void kvs_cursor_free(KVSCursor *cur);

/* カーソルを先頭/末尾/指定キーに移動 */
int kvs_cursor_first(KVSCursor *cur);
int kvs_cursor_last(KVSCursor *cur);
int kvs_cursor_seek(KVSCursor *cur, const char *key);

/* カーソル移動 */
int kvs_cursor_next(KVSCursor *cur);
int kvs_cursor_prev(KVSCursor *cur);
int kvs_cursor_valid(KVSCursor *cur);

/* カーソル位置のデータ取得 */
const char *kvs_cursor_key(KVSCursor *cur, size_t *klen);
const char *kvs_cursor_value(KVSCursor *cur, size_t *vlen);

/* ===== Persistence ===== */
int kvs_save(KVS *db, const char *path);
KVS *kvs_load(const char *path);

/* ===== Utilities ===== */
void kvs_stats(KVS *db, KVSStats *stats);

typedef void (*kvs_iter_fn)(const char *key, size_t klen,
                            const char *value, size_t vlen,
                            void *userdata);
size_t kvs_foreach(KVS *db, kvs_iter_fn callback, void *userdata);

/* Range iteration: from <= key <= to */
size_t kvs_range(KVS *db, const char *from, const char *to,
                 kvs_iter_fn callback, void *userdata);

#ifdef __cplusplus
}
#endif

#endif /* KVS_H */