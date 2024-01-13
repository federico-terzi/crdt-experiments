# json-crdt-rust

An experimental, high-performance JSON CRDT written in Rust. 

It features:
* Custom B-tree structure for efficient, real-time text editing.
* Columnar compression for efficient storage of the document.
* Cached views, for instant load of large documents when read-only access is enough.
* Map and Text data structures

Note: not production ready, highly experimental.

# Getting started

```
cargo run --release --example paper_trace
```