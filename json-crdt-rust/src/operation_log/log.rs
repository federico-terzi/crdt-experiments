use std::{cmp::Ordering, collections::VecDeque};

use bytes::Bytes;
use rustc_hash::FxHashMap;
use thiserror::Error;

use crate::{
    client_registry::{ClientRemappable, ClientRemappings},
    operation_log::serde::deserialize_operations,
    serde::{Serializable, SerializationError},
    ClientId, Operation, OperationAction, OperationId, SequenceIndex, Timestamp,
};

use super::{serde::serialize_operations, shared::OperationIndex};

#[derive(Clone)]
pub struct OperationLog {
    local_client: ClientId,
    operations: Vec<Operation>,
    client_sequences: FxHashMap<ClientId, SequenceIndex>,
    id_to_index: FxHashMap<OperationId, OperationIndex>,
    roots: Vec<OperationIndex>,
    last: Option<OperationIndex>,
    orphans: FxHashMap<OperationId, Operation>,
}

impl OperationLog {
    pub fn new(local_client: ClientId) -> Self {
        Self {
            local_client,
            operations: Vec::new(),
            client_sequences: FxHashMap::default(),
            id_to_index: FxHashMap::default(),
            roots: Vec::new(),
            last: None,
            orphans: FxHashMap::default(),
        }
    }

    pub fn from_buffer(
        local_client: ClientId,
        remappings: Option<ClientRemappings>,
        buffer: &mut Bytes,
    ) -> Result<Self, OperationLogError> {
        let mut operations = deserialize_operations(buffer)?;

        if let Some(remappings) = remappings {
            for operation in operations.iter_mut() {
                operation.remap_client_ids(&remappings);
            }
        }

        Self::load(local_client, operations)
    }

    fn load(local_client: ClientId, operations: Vec<Operation>) -> Result<Self, OperationLogError> {
        let mut operation_log = Self::new(local_client);

        for operation in operations {
            operation_log.apply_operation(operation)?;
        }

        Ok(operation_log)
    }

    pub fn apply_local_action(
        &mut self,
        action: OperationAction,
        timestamp: Timestamp,
    ) -> Result<&Operation, OperationLogError> {
        let operation = Operation {
            id: self.next_id(),
            parent: self.last.map(|index| self.operations[index].id.clone()),
            action,
            timestamp,
        };

        let inserted = self
            .insert_operation(operation)?
            .expect("operation should have been inserted");
        let operation = &self.operations[inserted];
        Ok(operation)
    }

    pub fn apply_operation(&mut self, op: Operation) -> Result<Vec<&Operation>, OperationLogError> {
        let mut applied_operations = Vec::new();

        let mut operation_id = op.id.clone();

        if let Some(applied_operation) = self.insert_operation(op)? {
            applied_operations.push(applied_operation);
        }

        // Process any orphans
        loop {
            let orphan = match self.orphans.remove(&operation_id) {
                Some(orphan) => orphan,
                None => break,
            };

            operation_id = orphan.id;

            if let Some(applied_operation) = self.insert_operation(orphan)? {
                applied_operations.push(applied_operation);
            }
        }

        Ok(applied_operations
            .iter()
            .map(|index| &self.operations[*index])
            .collect())
    }

    pub fn iter(&self) -> impl Iterator<Item = &Operation> {
        self.operations.iter()
    }

    pub fn iter_sorted(&self) -> impl Iterator<Item = &Operation> {
        SortedOperationIterator::new(&self.roots, &self.operations, &self.id_to_index)
    }

    fn insert_operation(
        &mut self,
        op: Operation,
    ) -> Result<Option<OperationIndex>, OperationLogError> {
        // Already processed
        if self.id_to_index.contains_key(&op.id) {
            return Ok(None);
        }

        // Orphan entry, we don't have the necessary dependencies yet
        if self.is_orphan(&op) {
            let op_parent = op.parent.expect("orphan should have a parent");
            self.orphans.insert(op_parent, op);
            return Ok(None);
        }

        let index = self.operations.len();
        self.id_to_index.insert(op.id.clone(), index);

        if op.parent.is_none() {
            self.roots.push(index);
        }

        // Update client sequences
        if let Some(sequence) = self.client_sequences.get(&op.id.client_id) {
            if op.id.sequence <= *sequence {
                panic!("sequence is not monotonically increasing");
            }
        }

        self.client_sequences
            .insert(op.id.client_id, op.id.sequence);

        // TODO: is the operation concurrent? If yes, we need to re-sort the entries
        if self.is_concurrent(&op) {
            self.operations.push(op);
            self.recalculate_last();
        } else {
            self.operations.push(op);
            self.last = Some(index);
        }

        Ok(Some(index))
    }

    fn is_orphan(&self, op: &Operation) -> bool {
        if let Some(parent) = op.parent.as_ref() {
            if !self.id_to_index.contains_key(parent) {
                return true;
            }
        }

        false
    }

    fn is_concurrent(&self, op: &Operation) -> bool {
        if let Some(last) = self.last {
            if let Some(parent) = op.parent.as_ref() {
                if self.operations[last].id == *parent {
                    return false;
                }
            }
        }

        true
    }

    fn recalculate_last(&mut self) {
        self.last = self.iter_sorted().last().map(|op| {
            self.id_to_index
                .get(&op.id)
                .expect("operation should have an index")
                .clone()
        });
    }

    fn next_id(&self) -> OperationId {
        let sequence = self.client_sequences.get(&self.local_client).unwrap_or(&0) + 1;

        OperationId {
            client_id: self.local_client,
            sequence,
        }
    }
}

impl Serializable for OperationLog {
    fn serialize(&self) -> Result<Vec<u8>, SerializationError> {
        let all_operations = self.operations.iter().chain(self.orphans.values());
        let serialized = serialize_operations(all_operations)?;
        Ok(serialized)
    }
}

pub struct SortedOperationIterator<'a> {
    operations: &'a [Operation],
    children: FxHashMap<OperationIndex, Vec<OperationIndex>>,
    to_visit: VecDeque<OperationIndex>,
}

impl<'a> SortedOperationIterator<'a> {
    pub fn new(
        roots: &'a [OperationIndex],
        operations: &'a [Operation],
        id_to_index: &'a FxHashMap<OperationId, OperationIndex>,
    ) -> Self {
        let mut to_visit: VecDeque<OperationIndex> = VecDeque::new();
        let mut roots = Vec::from(roots);
        roots.sort_by(|a, b| Self::compare_operations(*a, *b, operations));
        to_visit.extend(roots);

        let mut children: FxHashMap<OperationIndex, Vec<OperationIndex>> = FxHashMap::default();
        for (index, operation) in operations.iter().enumerate() {
            if let Some(parent) = operation.parent {
                let parent_index = id_to_index[&parent];
                children.entry(parent_index).or_default().push(index);
            }
        }

        Self {
            operations,
            children,
            to_visit,
        }
    }

    fn compare_operations(
        a: OperationIndex,
        b: OperationIndex,
        operations: &'a [Operation],
    ) -> Ordering {
        let a_operation = &operations[a];
        let b_operation = &operations[b];
        let a_id = a_operation.id;
        let b_id = b_operation.id;

        if a_id.client_id == b_id.client_id {
            a_id.sequence.cmp(&b_id.sequence)
        } else if a_operation.timestamp == b_operation.timestamp {
            a_id.client_id.cmp(&b_id.client_id)
        } else {
            a_operation.timestamp.cmp(&b_operation.timestamp)
        }
    }
}

impl<'a> Iterator for SortedOperationIterator<'a> {
    type Item = &'a Operation;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(index) = self.to_visit.pop_back() {
            match self.children.get(&index) {
                Some(children) if children.len() == 1 => {
                    self.to_visit.push_back(children[0]);
                }
                Some(children) => {
                    let mut children_copy = children.clone();
                    children_copy
                        .sort_by(|a, b| Self::compare_operations(*a, *b, &self.operations));

                    for child in children_copy {
                        self.to_visit.push_back(child);
                    }
                }
                None => {}
            }

            return Some(&self.operations[index]);
        }

        None
    }
}

#[derive(Error, Debug)]
pub enum OperationLogError {
    #[error("serialization error: {0}")]
    SerializationError(#[from] SerializationError),
}

impl ClientRemappable for OperationLog {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        self.local_client = mappings
            .get(&self.local_client)
            .expect("local client ID not found")
            .clone();

        for operation in self.operations.iter_mut() {
            operation.remap_client_ids(mappings);
        }

        let mut new_client_sequences = FxHashMap::default();
        for (client_id, sequence) in self.client_sequences.iter() {
            let new_client_id = mappings
                .get(client_id)
                .expect("client ID not found")
                .clone();
            new_client_sequences.insert(new_client_id, *sequence);
        }
        self.client_sequences = new_client_sequences;

        let mut new_id_to_index = FxHashMap::default();
        for (id, index) in self.id_to_index.iter() {
            let new_client_id = mappings
                .get(&id.client_id)
                .expect("client ID not found")
                .clone();
            let new_id = OperationId {
                client_id: new_client_id,
                sequence: id.sequence,
            };
            new_id_to_index.insert(new_id, *index);
        }
        self.id_to_index = new_id_to_index;

        let mut new_orphans = FxHashMap::default();
        for (id, operation) in self.orphans.iter() {
            let new_client_id = mappings
                .get(&id.client_id)
                .expect("client ID not found")
                .clone();
            let new_id = OperationId {
                client_id: new_client_id,
                sequence: id.sequence,
            };
            new_orphans.insert(new_id, operation.clone());
        }
        self.orphans = new_orphans;
    }
}
