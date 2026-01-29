/*
 * KVS - High Performance Key-Value Store Library
 * 
 * Features:
 *   - Hybrid Hash/RBTree auto-switching
 *   - Dynamic Bloom filter
 *   - Memory pool allocation
 *   - File persistence
 * 
 * Usage:
 *   KVS *db = kvs_open(NULL);        // In-memory
 *   KVS *db = kvs_open("data.kvs");  // Persistent
 *   kvs_put(db, "key", "value");
 *   char *v = kvs_get(db, "key");
 *   free(v);
 *   kvs_close(db);
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
#define KVS_DEFAULT_POOL_SIZE    (128 * 1024 * 1024)  /* 128MB */
#define KVS_DEFAULT_HASH_BUCKETS (8 * 1024)           /* 8K buckets */
#define KVS_DEFAULT_THRESHOLD    (KVS_DEFAULT_HASH_BUCKETS * 8)
#define KVS_BLOOM_INIT_BITS      (1 << 20)            /* 1M bits */
#define KVS_BLOOM_MAX_BITS       (1 << 26)            /* 64M bits */

/* ===== Types ===== */
typedef struct KVS KVS;

typedef enum {
    KVS_OK = 0,
    KVS_ERR_NOMEM = -1,
    KVS_ERR_NOTFOUND = -2,
    KVS_ERR_IO = -3,
    KVS_ERR_FULL = -4
} KVSError;

typedef enum {
    KVS_MODE_HASH,
    KVS_MODE_RBTREE
} KVSMode;

typedef struct {
    size_t count;           /* Total entries */
    size_t memory_used;     /* Pool memory used */
    size_t bloom_bits;      /* Current bloom size */
    double bloom_fill_rate; /* Bloom fill percentage */
    KVSMode mode;           /* Current mode */
} KVSStats;

/* ===== Core API ===== */

/**
 * Open or create a KVS database
 * @param path File path for persistence, NULL for in-memory only
 * @return KVS handle, NULL on error
 */
KVS *kvs_open(const char *path);

/**
 * Close and free KVS database
 * @param db KVS handle
 */
void kvs_close(KVS *db);

/**
 * Store a key-value pair
 * @param db KVS handle
 * @param key Null-terminated key string
 * @param value Null-terminated value string
 * @return KVS_OK on success, error code on failure
 */
int kvs_put(KVS *db, const char *key, const char *value);

/**
 * Store a key-value pair with explicit lengths (binary safe)
 * @param db KVS handle
 * @param key Key data
 * @param klen Key length
 * @param value Value data
 * @param vlen Value length
 * @return KVS_OK on success, error code on failure
 */
int kvs_put_raw(KVS *db, const char *key, size_t klen, const char *value, size_t vlen);

/**
 * Retrieve a value by key
 * @param db KVS handle
 * @param key Null-terminated key string
 * @return Newly allocated value string (caller must free), NULL if not found
 */
char *kvs_get(KVS *db, const char *key);

/**
 * Retrieve a value with explicit key length (binary safe)
 * @param db KVS handle
 * @param key Key data
 * @param klen Key length
 * @param vlen Output: value length (can be NULL)
 * @return Newly allocated value (caller must free), NULL if not found
 */
char *kvs_get_raw(KVS *db, const char *key, size_t klen, size_t *vlen);

/**
 * Check if a key exists (faster than kvs_get)
 * @param db KVS handle
 * @param key Null-terminated key string
 * @return 1 if exists, 0 if not
 */
int kvs_exists(KVS *db, const char *key);

/**
 * Delete a key-value pair
 * @param db KVS handle
 * @param key Null-terminated key string
 * @return KVS_OK on success, KVS_ERR_NOTFOUND if not exists
 */
int kvs_delete(KVS *db, const char *key);

/* ===== Persistence ===== */

/**
 * Save database to file
 * @param db KVS handle
 * @param path File path
 * @return KVS_OK on success, KVS_ERR_IO on failure
 */
int kvs_save(KVS *db, const char *path);

/**
 * Load database from file
 * @param path File path
 * @return KVS handle, NULL on error
 */
KVS *kvs_load(const char *path);

/* ===== Utilities ===== */

/**
 * Get database statistics
 * @param db KVS handle
 * @param stats Output statistics structure
 */
void kvs_stats(KVS *db, KVSStats *stats);

/**
 * Get current mode as string
 * @param db KVS handle
 * @return "Hash" or "RBTree"
 */
const char *kvs_mode_str(KVS *db);

/**
 * Force conversion to RBTree mode
 * @param db KVS handle
 */
void kvs_compact(KVS *db);

/**
 * Iterate over all entries
 * @param db KVS handle
 * @param callback Function called for each entry
 * @param userdata User data passed to callback
 * @return Number of entries iterated
 */
typedef void (*kvs_iter_fn)(const char *key, size_t klen, 
                            const char *value, size_t vlen, 
                            void *userdata);
size_t kvs_foreach(KVS *db, kvs_iter_fn callback, void *userdata);

#ifdef __cplusplus
}
#endif

#endif /* KVS_H */