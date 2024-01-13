use bytes::Bytes;
use peak_alloc::PeakAlloc;

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

use std::time::Instant;

use json_crdt_rust::{Doc, ObjRef, ReadableDoc, WritableDoc};
use serde_json::Value;

enum Edit {
    Insert(usize, String),
    Delete(usize, usize),
}

fn execute_trace(edits: Vec<Edit>) -> Doc {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();

    for (index, edit) in edits.iter().enumerate() {
        match edit {
            Edit::Insert(index, content) => txn.insert_text(&text, *index as u32, content).unwrap(),
            Edit::Delete(index, count) => txn
                .delete_text(&text, *index as u32, *count as u32)
                .unwrap(),
        }
    }

    txn.commit().unwrap();

    doc
}

fn load_edits() -> Vec<Edit> {
    let trace_file_content = include_str!("../benches/automerge-trace/trace.json");
    let trace: Value = serde_json::from_str(trace_file_content).unwrap();

    let mut edits = Vec::new();

    for edit in trace.as_array().unwrap() {
        let action = edit.as_array().unwrap();
        if action[1] == 0 {
            edits.push(Edit::Insert(
                action[0].as_i64().unwrap() as usize,
                action[2].as_str().unwrap().to_owned(),
            ));
        } else {
            edits.push(Edit::Delete(
                action[0].as_i64().unwrap() as usize,
                action[1].as_i64().unwrap() as usize,
            ));
        }
    }

    edits
}

fn load_expected_string() -> String {
    let expected_content = include_str!("../benches/automerge-trace/final.json");
    let content: Value = serde_json::from_str(expected_content).unwrap();
    content.as_str().unwrap().to_owned()
}

fn main() {
    println!("creating document...");
    let edits = load_edits();
    let before_trace = PEAK_ALLOC.peak_usage_as_mb();
    let mut doc = execute_trace(edits);

    println!("Starting benchmark");
    let start = Instant::now();

    let serialized = doc.serialize().unwrap();
    let serialized = Bytes::from(serialized);

    let duration = start.elapsed();
    println!("Time elapsed for serialization () is: {:?}", duration);
    println!("Serialization size: {} bytes", serialized.len());

    let after_trace = PEAK_ALLOC.peak_usage_as_mb();
    println!(
        "Estimated peak memory usage {}mb",
        after_trace - before_trace
    );

    // std::fs::write("test.bin", &serialized);

    println!("Starting deserialization...");
    let start = Instant::now();

    let deserialized_doc = Doc::load("2".to_string(), serialized.clone()).unwrap();

    let duration = start.elapsed();
    let after_deserialization = PEAK_ALLOC.peak_usage_as_mb();
    println!("Time elapsed for deserialization () is: {:?}", duration);
    println!(
        "Estimated peak memory usage by deserialization {}mb",
        after_deserialization - after_trace
    );

    println!("Lazy deserialization...");
    let start = Instant::now();

    let lazy_doc = Doc::lazy("3".to_string(), serialized);

    let duration = start.elapsed();
    let after_lazy_deserialization = PEAK_ALLOC.peak_usage_as_mb();
    println!(
        "Time elapsed for lazy deserialization () is: {:?}",
        duration
    );
    println!(
        "Estimated peak memory usage by lazy deserialization {}mb",
        after_lazy_deserialization - after_deserialization
    );

    // TODO: test document equality
    // TODO: test lazy document loading
    // assert_eq!(doc, deserialized_doc);
}
