use criterion::{black_box, criterion_group, criterion_main, Criterion};
use json_crdt_rust::{Doc, ObjRef, ReadableDoc, WritableDoc};

fn simple_insertions(n: u64) {
    let mut doc = Doc::new("1".to_string());

    for i in 0..n {
        let mut txn = doc.transaction();
        txn.set_scalar(ObjRef::Root, format!("field_{}", i), "value")
            .unwrap();
        txn.commit().unwrap();
    }

    let value = doc
        .get(ObjRef::Root, format!("field_{}", n - 1))
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();

    assert_eq!(value.as_string().unwrap(), "value");
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("simple-insertions", |b| {
        b.iter(|| simple_insertions(black_box(10000)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
