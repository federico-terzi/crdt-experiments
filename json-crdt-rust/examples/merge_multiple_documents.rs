use json_crdt_rust::{Doc, ObjRef, ReadableDoc, WritableDoc};

fn simple_merge(edits: u64, replicas: u32) {
    let mut docs = Vec::new();

    for replica in 0..replicas {
        let mut doc = Doc::new(replica.to_string());

        for i in 0..edits {
            let mut txn = doc.transaction();
            txn.set_scalar(ObjRef::Root, format!("field_{}", i), "value")
                .unwrap();
            txn.commit().unwrap();
        }

        let value = doc
            .get(ObjRef::Root, format!("field_{}", edits - 1))
            .unwrap()
            .unwrap()
            .as_scalar()
            .unwrap();

        assert_eq!(value.as_string().unwrap(), "value");

        docs.push(doc);
    }

    let mut first_doc = docs.remove(0);

    for doc in docs {
        first_doc.merge(&doc).unwrap();
    }
}

fn main() {
    println!("Starting benchmark");
    for _ in 0..10 {
        simple_merge(10000, 3);
    }
}
