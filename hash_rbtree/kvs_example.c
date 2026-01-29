/*
 * KVS Usage Example
 * 
 * Compile:
 *   gcc -O3 -o kvs_example kvs_example.c kvs.c -lm
 * 
 * Run:
 *   ./kvs_example
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "kvs.h"

double now_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

void print_entry(const char *key, size_t klen, const char *value, size_t vlen, void *arg) {
    int *count = (int*)arg;
    if (*count < 5) printf("  %.*s = %.*s\n", (int)klen, key, (int)vlen, value);
    (*count)++;
}

int main() {
    printf("=== KVS Library Example ===\n\n");
    
    /* 1. Basic Usage */
    printf("1. Basic Usage\n");
    printf("─────────────────────────────────────\n");
    
    KVS *db = kvs_open(NULL);  /* In-memory */
    
    kvs_put(db, "name", "Alice");
    kvs_put(db, "age", "30");
    kvs_put(db, "city", "Tokyo");
    
    char *name = kvs_get(db, "name");
    char *age = kvs_get(db, "age");
    printf("  name = %s\n", name);
    printf("  age = %s\n", age);
    free(name);
    free(age);
    
    printf("  exists('city') = %d\n", kvs_exists(db, "city"));
    printf("  exists('country') = %d\n", kvs_exists(db, "country"));
    
    kvs_delete(db, "age");
    char *deleted = kvs_get(db, "age");
    printf("  after delete, age = %s\n", deleted ? deleted : "(null)");
    free(deleted);
    
    kvs_close(db);
    
    /* 2. Benchmark */
    printf("\n2. Performance Benchmark\n");
    printf("─────────────────────────────────────\n");
    
    int N = 100000;
    db = kvs_open(NULL);
    
    char key[32], val[64];
    double t0;
    
    /* Write */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        sprintf(key, "key_%08d", i);
        sprintf(val, "value_%d", i);
        kvs_put(db, key, val);
    }
    printf("  Write %d: %.2f ops/sec\n", N, N / (now_sec() - t0));
    
    /* Read */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        sprintf(key, "key_%08d", i);
        char *v = kvs_get(db, key);
        free(v);
    }
    printf("  Read %d: %.2f ops/sec\n", N, N / (now_sec() - t0));
    
    /* Miss */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        sprintf(key, "miss_%08d", i);
        char *v = kvs_get(db, key);
        free(v);
    }
    printf("  Miss %d: %.2f ops/sec\n", N, N / (now_sec() - t0));
    
    /* Stats */
    KVSStats stats;
    kvs_stats(db, &stats);
    printf("\n  Stats:\n");
    printf("    Mode: %s\n", kvs_mode_str(db));
    printf("    Count: %zu\n", stats.count);
    printf("    Memory: %.2f MB\n", stats.memory_used / (1024.0 * 1024.0));
    printf("    Bloom: %zuK bits (%.1f%% full)\n", stats.bloom_bits / 1024, stats.bloom_fill_rate);
    
    kvs_close(db);
    
    /* 3. Persistence */
    printf("\n3. Persistence\n");
    printf("─────────────────────────────────────\n");
    
    db = kvs_open(NULL);
    kvs_put(db, "persistent_key", "persistent_value");
    kvs_put(db, "another_key", "another_value");
    kvs_save(db, "test.kvs");
    printf("  Saved to test.kvs\n");
    kvs_close(db);
    
    db = kvs_load("test.kvs");
    if (db) {
        printf("  Loaded from test.kvs\n");
        kvs_stats(db, &stats);
        printf("  Count: %zu\n", stats.count);
        kvs_close(db);
    }
    remove("test.kvs");
    
    /* 4. Iteration */
    printf("\n4. Iteration\n");
    printf("─────────────────────────────────────\n");
    
    db = kvs_open(NULL);
    kvs_put(db, "apple", "red");
    kvs_put(db, "banana", "yellow");
    kvs_put(db, "grape", "purple");
    kvs_put(db, "orange", "orange");
    kvs_put(db, "melon", "green");
    
    printf("  All entries:\n");
    int count = 0;
    kvs_foreach(db, print_entry, &count);
    
    kvs_close(db);
    
    /* 5. Binary Data */
    printf("\n5. Binary Data\n");
    printf("─────────────────────────────────────\n");
    
    db = kvs_open(NULL);
    
    char binary_key[] = {0x01, 0x02, 0x03, 0x00, 0x04};  /* Contains null byte */
    char binary_val[] = {0xFF, 0xFE, 0x00, 0xFD, 0xFC};
    
    kvs_put_raw(db, binary_key, 5, binary_val, 5);
    
    size_t vlen;
    char *retrieved = kvs_get_raw(db, binary_key, 5, &vlen);
    printf("  Binary value length: %zu\n", vlen);
    printf("  Binary value bytes: ");
    for (size_t i = 0; i < vlen; i++) printf("%02X ", (unsigned char)retrieved[i]);
    printf("\n");
    free(retrieved);
    
    kvs_close(db);
    
    printf("\n=== Done ===\n");
    return 0;
}