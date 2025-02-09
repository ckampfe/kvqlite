# kvqlite

[![Rust](https://github.com/ckampfe/kvqlite/actions/workflows/rust.yml/badge.svg)](https://github.com/ckampfe/kvqlite/actions/workflows/rust.yml)

key/value db backed by sqlite, with two storage strategies: update-in-place, and append.

## update in place

- writes update values in place
- deletes remove the entire key/value from the database
- reads read the given key and value

```rust
let db: Db<UpdateInPlace> = Db::builder().in_memory().finish().await.unwrap();

db.write("hello", "world").await.unwrap();
let value: String = db.read("hello").await.unwrap().unwrap();

assert_eq!(value, "world");
```

## append

- writes append a new value that references the given key
- deletes remove the key and all associated values
- reads read the given key and the latest value associated with the given key

```rust
let db: Db<Append> = Db::builder().in_memory().finish().await.unwrap();

db.write("hello", "world").await.unwrap();
let value: String = db.read("hello").await.unwrap().unwrap();
assert_eq!(value, "world");

tokio::time::sleep(std::time::Duration::from_millis(1)).await;

db.write("hello", "joe").await.unwrap();
let value: String = db.read("hello").await.unwrap().unwrap();
assert_eq!(value, "joe");

let count = db.entries_count().await.unwrap();
assert_eq!(count, 2);

let keys_count = db.keys_count().await.unwrap();
assert_eq!(keys_count, 1);
```
