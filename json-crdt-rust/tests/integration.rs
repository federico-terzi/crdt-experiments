use json_crdt_rust::{Doc, ObjRef, ReadableDoc, WritableDoc};

#[test]
fn create_document() {
    let _doc = Doc::new("1".to_string());
}

#[test]
fn set_and_get_string() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    txn.set_scalar(ObjRef::Root, "field", "value").unwrap();
    txn.commit().unwrap();

    let value = doc
        .get(ObjRef::Root, "field")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value.as_string().unwrap(), "value");
}

#[test]
fn set_and_delete_string() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    txn.set_scalar(ObjRef::Root, "field", "value").unwrap();
    txn.commit().unwrap();

    let value = doc
        .get(ObjRef::Root, "field")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value.as_string().unwrap(), "value");

    let mut txn = doc.transaction();
    txn.delete(ObjRef::Root, "field").unwrap();
    txn.commit().unwrap();

    let value = doc.get(ObjRef::Root, "field").unwrap();
    assert!(value.is_none());
}

#[test]
fn set_and_get_string_numeric_index() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    txn.set_scalar(ObjRef::Root, 123, "value").unwrap();
    txn.commit().unwrap();

    let value = doc
        .get(ObjRef::Root, 123)
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value.as_string().unwrap(), "value");
}

#[test]
fn set_and_get_multiple_times() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    txn.set_scalar(ObjRef::Root, "field", "value").unwrap();
    txn.set_scalar(ObjRef::Root, "another", "test").unwrap();
    txn.commit().unwrap();

    let value1 = doc
        .get(ObjRef::Root, "field")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value1.as_string().unwrap(), "value");

    let value2 = doc
        .get(ObjRef::Root, "another")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value2.as_string().unwrap(), "test");
}

#[test]
fn set_and_get_multiple_transactions() {
    let mut doc = Doc::new("1".to_string());

    let mut txn1 = doc.transaction();
    txn1.set_scalar(ObjRef::Root, "field", "value").unwrap();
    txn1.commit().unwrap();

    let mut txn2 = doc.transaction();
    txn2.set_scalar(ObjRef::Root, "another", "test").unwrap();
    txn2.commit().unwrap();

    let value1 = doc
        .get(ObjRef::Root, "field")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value1.as_string().unwrap(), "value");

    let value2 = doc
        .get(ObjRef::Root, "another")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value2.as_string().unwrap(), "test");
}

#[test]
fn create_and_set_nested_map() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let map = txn.create_map(ObjRef::Root, "nested_map").unwrap();
    txn.set_scalar(&map, "field", "value").unwrap();
    txn.commit().unwrap();

    let value = doc
        .get(&map, "field")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value.as_string().unwrap(), "value");
}

#[test]
fn create_and_append_text() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.append_text(&text, "hello ").unwrap();
    txn.append_text(&text, "world").unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "hello world");
}

#[test]
fn append_and_insert_text() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.append_text(&text, "hello world").unwrap();
    txn.insert_text(&text, 5, " beautiful").unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "hello beautiful world");
}

#[test]
fn append_and_delete_text() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.append_text(&text, "hello world").unwrap();
    txn.delete_text(&text, 8, 3).unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "hello wo");
}

#[test]
fn insert_sequence() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.insert_text(&text, 0, "h").unwrap();
    txn.insert_text(&text, 1, "e").unwrap();
    txn.insert_text(&text, 2, "l").unwrap();
    txn.insert_text(&text, 3, "l").unwrap();
    txn.insert_text(&text, 4, "o").unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "hello");
}

#[test]
fn insert_overlapping_position() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.insert_text(&text, 0, "h").unwrap();
    txn.insert_text(&text, 1, "e").unwrap();
    txn.insert_text(&text, 2, "l").unwrap();
    txn.insert_text(&text, 1, "z").unwrap();
    txn.insert_text(&text, 3, "y").unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "hzeyl");
}

#[test]
fn insert_sequence_multiple_transactions() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.insert_text(&text, 0, "h").unwrap();
    txn.commit().unwrap();

    let mut txn = doc.transaction();
    let text = txn.get_text(ObjRef::Root, "text").unwrap().unwrap();
    txn.insert_text(&text, 1, "e").unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "he");
}

#[test]
fn insert_and_delete_sequence() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.insert_text(&text, 0, "h").unwrap();
    txn.insert_text(&text, 1, "e").unwrap();
    txn.insert_text(&text, 2, "l").unwrap();
    txn.insert_text(&text, 3, "l").unwrap();
    txn.insert_text(&text, 4, "o").unwrap();
    txn.delete_text(&text, 4, 1).unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "hell");
}

#[test]
fn insert_and_delete_inside() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.insert_text(&text, 0, "h").unwrap();
    txn.insert_text(&text, 1, "e").unwrap();
    txn.insert_text(&text, 2, "l").unwrap();
    txn.insert_text(&text, 3, "l").unwrap();
    txn.insert_text(&text, 4, "o").unwrap();
    txn.delete_text(&text, 1, 2).unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "hlo");
}

#[test]
fn delete_across_boundaries() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.insert_text(&text, 0, "hello").unwrap();
    txn.insert_text(&text, 5, " world").unwrap();
    txn.insert_text(&text, 11, "!").unwrap();
    txn.delete_text(&text, 3, 4).unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "helorld!");
}

#[test]
fn insert_after_delete() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.insert_text(&text, 0, "hello").unwrap();
    txn.insert_text(&text, 5, " world").unwrap();
    txn.delete_text(&text, 3, 4).unwrap();
    txn.insert_text(&text, 3, "lo w").unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "hello world");
}

#[test]
fn insert_between_delete() {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();
    txn.insert_text(&text, 0, "hello").unwrap();
    txn.insert_text(&text, 5, " world").unwrap();
    txn.delete_text(&text, 3, 4).unwrap();
    txn.insert_text(&text, 5, "y").unwrap();
    txn.commit().unwrap();

    let value = doc.get_text(text).unwrap().unwrap();
    assert_eq!(value.to_string(), "heloryld");
}

#[test]
fn merging_two_documents_merges_top_level_fields() {
    let mut doc1 = Doc::new("1".to_string());
    let mut doc2 = Doc::new("2".to_string());

    let mut txn1 = doc1.transaction();
    txn1.set_scalar(ObjRef::Root, "first", "foo").unwrap();
    txn1.commit().unwrap();

    let mut txn2 = doc2.transaction();
    txn2.set_scalar(ObjRef::Root, "second", "bar").unwrap();
    txn2.commit().unwrap();

    doc1.merge(&doc2).unwrap();

    let value1 = doc1
        .get(ObjRef::Root, "second")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value1.as_string().unwrap(), "bar");

    let value2 = doc1
        .get(ObjRef::Root, "first")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value2.as_string().unwrap(), "foo");
}

#[test]
fn merge_does_converge_root_changes() {
    let mut doc1 = Doc::new("1".to_string());
    let mut doc2 = Doc::new("2".to_string());

    let mut txn1 = doc1.transaction();
    txn1.set_scalar(ObjRef::Root, "register", "foo").unwrap();
    txn1.commit().unwrap();

    let mut txn2 = doc2.transaction();
    txn2.set_scalar(ObjRef::Root, "register", "bar").unwrap();
    txn2.commit().unwrap();

    doc1.merge(&doc2).unwrap();
    doc2.merge(&doc1).unwrap();

    let value1 = doc1.get(ObjRef::Root, "register").unwrap().unwrap();
    let value2 = doc2.get(ObjRef::Root, "register").unwrap().unwrap();

    assert_eq!(value1, value2);
}

#[test]
fn merge_does_converge_subsequent_transaction() {
    let mut doc1 = Doc::new("1".to_string());
    let mut doc2 = Doc::new("2".to_string());

    let mut txn1 = doc1.transaction();
    txn1.set_scalar(ObjRef::Root, "register", "one").unwrap();
    txn1.commit().unwrap();

    doc2.merge(&doc1).unwrap();

    let mut txn1 = doc1.transaction();
    txn1.set_scalar(ObjRef::Root, "register", "two").unwrap();
    txn1.commit().unwrap();

    let mut txn2 = doc2.transaction();
    txn2.set_scalar(ObjRef::Root, "register", "three").unwrap();
    txn2.commit().unwrap();

    doc1.merge(&doc2).unwrap();
    doc2.merge(&doc1).unwrap();

    let value1 = doc1.get(ObjRef::Root, "register").unwrap().unwrap();
    let value2 = doc2.get(ObjRef::Root, "register").unwrap().unwrap();

    assert_eq!(value1, value2);
}

#[test]
fn merge_three_totally_concurrent_edit_chains() {
    let edits = 3;
    let replicas = 3;

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

#[test]
fn merge_map_deletes_do_not_overwrite_concurrent_set() {
    let mut doc1 = Doc::new("1".to_string());
    let mut doc2 = Doc::new("2".to_string());

    let mut txn1 = doc1.transaction();
    txn1.set_scalar(ObjRef::Root, "register", "foo").unwrap();
    txn1.commit().unwrap();

    doc2.merge(&doc1).unwrap();

    let value1 = doc1
        .get(ObjRef::Root, "register")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    let value2 = doc2
        .get(ObjRef::Root, "register")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();

    assert_eq!(value1, value2);
    assert_eq!(value1.as_string().unwrap(), "foo");

    let mut txn1 = doc1.transaction();
    txn1.delete(ObjRef::Root, "register").unwrap();
    txn1.commit().unwrap();

    let mut txn2 = doc2.transaction();
    txn2.set_scalar(ObjRef::Root, "register", "bar").unwrap();
    txn2.commit().unwrap();

    let value1 = doc1.get(ObjRef::Root, "register").unwrap();
    assert!(value1.is_none());
    let value2 = doc2
        .get(ObjRef::Root, "register")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    assert_eq!(value2.as_string().unwrap(), "bar");

    doc1.merge(&doc2).unwrap();
    doc2.merge(&doc1).unwrap();

    let value1 = doc1
        .get(ObjRef::Root, "register")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    let value2 = doc2
        .get(ObjRef::Root, "register")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();

    assert_eq!(value1, value2);
    assert_eq!(value1.as_string().unwrap(), "bar");
}

#[test]
fn merge_map_concurrent_deletes_are_confirmed() {
    let mut doc1 = Doc::new("1".to_string());
    let mut doc2 = Doc::new("2".to_string());

    let mut txn1 = doc1.transaction();
    txn1.set_scalar(ObjRef::Root, "register", "foo").unwrap();
    txn1.commit().unwrap();

    doc2.merge(&doc1).unwrap();

    let value1 = doc1
        .get(ObjRef::Root, "register")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();
    let value2 = doc2
        .get(ObjRef::Root, "register")
        .unwrap()
        .unwrap()
        .as_scalar()
        .unwrap();

    assert_eq!(value1, value2);
    assert_eq!(value1.as_string().unwrap(), "foo");

    let mut txn1 = doc1.transaction();
    txn1.delete(ObjRef::Root, "register").unwrap();
    txn1.commit().unwrap();

    let mut txn2 = doc2.transaction();
    txn2.delete(ObjRef::Root, "register").unwrap();
    txn2.commit().unwrap();

    doc1.merge(&doc2).unwrap();
    doc2.merge(&doc1).unwrap();

    let value1 = doc1.get(ObjRef::Root, "register").unwrap();
    let value2 = doc2.get(ObjRef::Root, "register").unwrap();

    assert!(value1.is_none());
    assert!(value2.is_none());
}
