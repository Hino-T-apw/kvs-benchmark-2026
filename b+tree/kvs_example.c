/*
 * KVS Usage Example (B+Tree Edition)
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

int main(int argc, char **argv) {
    int N = (argc > 1) ? atoi(argv[1]) : 100000;
    
    printf("=== KVS Library Example (B+Tree Edition) ===\n\n");
    
    /* 1. Basic Usage */
    printf("1. Basic Usage\n");
    printf("─────────────────────────────────────\n");
    
    KVS *db = kvs_open(NULL);
    
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
    
    /* 2. Performance Benchmark */
    printf("\n2. Performance Benchmark (%d records)\n", N);
    printf("─────────────────────────────────────\n");
    
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
    printf("  Write: %.2f ops/sec\n", N / (now_sec() - t0));
    
    /* Sequential Read */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        sprintf(key, "key_%08d", i);
        char *v = kvs_get(db, key);
        free(v);
    }
    printf("  Seq Read: %.2f ops/sec\n", N / (now_sec() - t0));
    
    /* Random Read */
    srand(12345);
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        sprintf(key, "key_%08d", rand() % N);
        char *v = kvs_get(db, key);
        free(v);
    }
    printf("  Rand Read: %.2f ops/sec\n", N / (now_sec() - t0));
    
    /* Miss Read */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        sprintf(key, "miss_%08d", i);
        char *v = kvs_get(db, key);
        free(v);
    }
    printf("  Miss Read: %.2f ops/sec\n", N / (now_sec() - t0));
    
    /* Stats */
    KVSStats stats;
    kvs_stats(db, &stats);
    printf("\n  Stats:\n");
    printf("    Count: %zu\n", stats.count);
    printf("    Memory: %.2f MB\n", stats.memory_used / (1024.0 * 1024.0));
    printf("    Tree Height: %zu\n", stats.tree_height);
    printf("    Node Count: %zu\n", stats.node_count);
    printf("    Bloom: %zuK bits (%.1f%% full)\n", stats.bloom_bits / 1024, stats.bloom_fill_rate);
    
    kvs_close(db);
    
    /* 3. Range Query (B+Tree特有) */
    printf("\n3. Range Query (B+Tree Feature)\n");
    printf("─────────────────────────────────────\n");
    
    db = kvs_open(NULL);
    kvs_put(db, "apple", "red");
    kvs_put(db, "banana", "yellow");
    kvs_put(db, "cherry", "red");
    kvs_put(db, "date", "brown");
    kvs_put(db, "elderberry", "purple");
    kvs_put(db, "fig", "purple");
    kvs_put(db, "grape", "purple");
    
    printf("  Range 'banana' to 'fig':\n");
    int count = 0;
    kvs_range(db, "banana", "fig", print_entry, &count);
    
    /* 4. Cursor (順序走査) */
    printf("\n4. Cursor Navigation\n");
    printf("─────────────────────────────────────\n");
    
    KVSCursor *cur = kvs_cursor_new(db);
    
    printf("  Forward (first 5):\n");
    kvs_cursor_first(cur);
    for (int i = 0; i < 5 && kvs_cursor_valid(cur); i++) {
        size_t klen, vlen;
        const char *k = kvs_cursor_key(cur, &klen);
        const char *v = kvs_cursor_value(cur, &vlen);
        printf("    %.*s = %.*s\n", (int)klen, k, (int)vlen, v);
        kvs_cursor_next(cur);
    }
    
    printf("  Backward (last 3):\n");
    kvs_cursor_last(cur);
    for (int i = 0; i < 3 && kvs_cursor_valid(cur); i++) {
        size_t klen, vlen;
        const char *k = kvs_cursor_key(cur, &klen);
        const char *v = kvs_cursor_value(cur, &vlen);
        printf("    %.*s = %.*s\n", (int)klen, k, (int)vlen, v);
        kvs_cursor_prev(cur);
    }
    
    kvs_cursor_free(cur);
    kvs_close(db);
    
    /* 5. Persistence */
    printf("\n5. Persistence\n");
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
        char *v = kvs_get(db, "persistent_key");
        printf("  persistent_key = %s\n", v ? v : "(null)");
        free(v);
        kvs_close(db);
    }
    remove("test.kvs");
    
    printf("\n=== Done ===\n");
    return 0;
}