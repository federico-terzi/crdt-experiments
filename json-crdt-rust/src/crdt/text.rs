use std::fmt::Debug;

use crate::{ClientId, DeleteTextAction, InsertTextAction, SequenceBlockId, SequenceIndex};

use super::shared::tree::{SequenceBlock, SequenceTree};

// TODO: fine-tune them
const BRANCH_SIZE: usize = 32;
const LEAF_SIZE: usize = 32;

#[derive(Clone, PartialEq)]
pub struct TextCRDT {
    client: ClientId,
    next_available_sequence: SequenceIndex,

    tree: SequenceTree<String, BRANCH_SIZE, LEAF_SIZE>,
}

type StringBlock = SequenceBlock<String>;

impl TextCRDT {
    pub fn new(client: ClientId) -> Self {
        Self {
            client,
            next_available_sequence: 0,
            tree: SequenceTree::new(),
        }
    }

    pub fn next_id(&mut self, length: u32) -> SequenceBlockId {
        let new_sequence = self.next_available_sequence;
        self.next_available_sequence = new_sequence + length;
        SequenceBlockId {
            client_id: self.client,
            sequence: new_sequence,
        }
    }

    pub fn insert(&mut self, action: &InsertTextAction) {
        // TODO: possible optimization, keep only one string copy (the one in the action)
        let block = StringBlock::new(action.id.clone(), action.value.clone(), action.left.clone());
        self.tree.insert(block);
    }

    pub fn delete(&mut self, action: &DeleteTextAction) {
        self.tree.delete(&action.left, &action.right);
    }

    pub fn find_block_starting_at(&self, position: u32) -> Option<SequenceBlockId> {
        self.tree.find_id_starting_at_position(position)
    }

    pub fn find_block_ending_at(&self, position: u32) -> Option<SequenceBlockId> {
        self.tree.find_id_ending_at_position(position)
    }

    pub fn last_block(&self) -> Option<SequenceBlockId> {
        self.tree.last_block()
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();

        for sub_str in self.tree.iter() {
            result.push_str(&sub_str);
        }

        result
    }
}

impl Debug for TextCRDT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string())
    }
}
