use rustc_hash::FxHashMap;

use crate::MapBlockId;

use super::shared::MapBlock;

type BlockIndex = usize;

#[derive(Debug, Clone, PartialEq)]
pub struct BlockSet {
    blocks: Vec<MapBlock>,
    id_to_index: FxHashMap<MapBlockId, BlockIndex>,
    block_children: FxHashMap<BlockIndex, Vec<BlockIndex>>,
}

impl BlockSet {
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            id_to_index: FxHashMap::default(),
            block_children: FxHashMap::default(),
        }
    }

    pub fn insert(&mut self, block: MapBlock) {
        let index = self.blocks.len();
        self.blocks.push(block);

        let block = &self.blocks[index];
        self.id_to_index.insert(block.id.clone(), index);

        // Initialize children
        self.block_children.entry(index).or_insert_with(Vec::new);

        for parent in &block.parents {
            let parent_index = self.id_to_index[&parent];
            self.block_children
                .entry(parent_index)
                .or_insert_with(Vec::new)
                .push(index);
        }
    }

    pub fn delete(&mut self, blocks: &[MapBlockId]) {
        for block in blocks {
            let block_index = self.id_to_index[block];
            self.blocks[block_index].deleted = true;
        }
    }

    pub fn get_latest_with_conflicts(&self) -> Option<Vec<&MapBlock>> {
        let block_indexes_without_children: Vec<BlockIndex> = self
            .block_children
            .iter()
            .filter(|(_, children)| children.is_empty())
            .map(|(index, _)| *index)
            .collect();

        let blocks_without_children: Vec<&MapBlock> = block_indexes_without_children
            .iter()
            .map(|index| &self.blocks[*index])
            .collect();

        if blocks_without_children.is_empty() {
            None
        } else {
            Some(blocks_without_children)
        }
    }

    pub fn get_latest(&self) -> Option<&MapBlock> {
        let mut latest = self.get_latest_with_conflicts()?;

        latest.sort_by(|a, b| {
            if a.id.client_id == b.id.client_id {
                a.id.sequence.cmp(&b.id.sequence)
            } else if a.timestamp == b.timestamp {
                a.id.client_id.cmp(&b.id.client_id)
            } else {
                a.timestamp.cmp(&b.timestamp)
            }
        });

        for block in latest.iter().rev() {
            if !block.deleted {
                return Some(block);
            }
        }

        None
    }
}
