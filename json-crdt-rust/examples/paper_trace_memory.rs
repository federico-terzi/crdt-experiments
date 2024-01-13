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

fn execute_trace(edits: Vec<Edit>) -> String {
    let mut doc = Doc::new("1".to_string());

    let mut txn = doc.transaction();
    let text = txn.create_text(ObjRef::Root, "text").unwrap();

    for (index, edit) in edits.iter().enumerate() {
        if index % 10000 == 0 {
            println!("{} / {}", index, edits.len());
        }

        match edit {
            Edit::Insert(index, content) => txn.insert_text(&text, *index as u32, content).unwrap(),
            Edit::Delete(index, count) => txn
                .delete_text(&text, *index as u32, *count as u32)
                .unwrap(),
        }
    }

    txn.commit().unwrap();

    let value = doc.get_text(&text).unwrap().unwrap();
    value.to_string()
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
    println!("loading edits...");
    let edits = load_edits();

    println!("loaded {} edits", edits.len());
    let before_trace = PEAK_ALLOC.peak_usage_as_mb();
    println!("Memory usage {}mb", before_trace);

    println!("Starting benchmark");

    let start = Instant::now();

    let actual_content = execute_trace(edits);

    let duration = start.elapsed();
    println!("Time elapsed in expensive_function() is: {:?}", duration);

    let after_trace = PEAK_ALLOC.peak_usage_as_mb();
    println!(
        "Estimated peak memory usage {}mb",
        after_trace - before_trace
    );

    let expected_content = load_expected_string();
    assert_eq!(actual_content, expected_content)
}
