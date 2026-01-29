# API LIST
```
/* Range Query */
kvs_range(db, "aaa", "zzz", callback, userdata);

/* Cursor */
KVSCursor *cur = kvs_cursor_new(db);
kvs_cursor_first(cur);      // 先頭へ
kvs_cursor_last(cur);       // 末尾へ
kvs_cursor_seek(cur, key);  // キーへジャンプ
kvs_cursor_next(cur);       // 次へ
kvs_cursor_prev(cur);       // 前へ
kvs_cursor_key(cur, &klen); // 現在のキー
kvs_cursor_value(cur, &vlen); // 現在の値
```
