# API LIST
```
/* Core */
KVS *kvs_open(const char *path);      // Open/Create
void kvs_close(KVS *db);               // Close
int kvs_put(KVS *db, key, value);      // Write
char *kvs_get(KVS *db, key);           // Read (要free)
int kvs_exists(KVS *db, key);          // Exists check
int kvs_delete(KVS *db, key);          // Delete

/* Binary Safe */
int kvs_put_raw(db, key, klen, value, vlen);
char *kvs_get_raw(db, key, klen, &vlen);

/* Persistence */
int kvs_save(KVS *db, path);
KVS *kvs_load(path);

/* Utilities */
void kvs_stats(KVS *db, &stats);
const char *kvs_mode_str(KVS *db);
void kvs_compact(KVS *db);
size_t kvs_foreach(db, callback, userdata);
```