use rustc_hash::FxHashMap;

use crate::{ClientId, MapBlockId, Selector, SequenceIndex, Timestamp, Value};

use super::{set::BlockSet, shared::MapBlock};

#[derive(Debug, Clone, PartialEq)]
pub struct MapCRDT {
    client: ClientId,
    next_available_sequence: SequenceIndex,
    fields: FxHashMap<Selector, BlockSet>,
}

pub struct SetParams {
    pub selector: Selector,
    pub id: MapBlockId,
    pub parents: Vec<MapBlockId>,
    pub value: Value,
    pub timestamp: Timestamp,
}

pub struct DeleteParams {
    pub selector: Selector,
    pub parents: Vec<MapBlockId>,
}

impl MapCRDT {
    pub fn new(client: ClientId) -> Self {
        Self {
            client,
            next_available_sequence: 0,
            fields: FxHashMap::default(),
        }
    }

    pub fn next_id(&mut self) -> MapBlockId {
        let new_sequence = self.next_available_sequence;
        self.next_available_sequence += 1;

        MapBlockId {
            client_id: self.client,
            sequence: new_sequence,
        }
    }

    pub fn get(&self, key: &Selector) -> Option<&Value> {
        let field = self.fields.get(key)?;
        let latest_block = field.get_latest()?;
        Some(&latest_block.value)
    }

    pub fn get_latest_ids(&self, key: &Selector) -> Vec<MapBlockId> {
        if let Some(field) = self.fields.get(key) {
            if let Some(latest_blocks) = field.get_latest_with_conflicts() {
                return latest_blocks
                    .iter()
                    .map(|block| block.id.clone())
                    .collect::<Vec<MapBlockId>>();
            }
        }

        Vec::new()
    }

    pub fn set(&mut self, action: SetParams) {
        let field = self
            .fields
            .entry(action.selector)
            .or_insert_with(BlockSet::new);

        let block = MapBlock {
            id: action.id,
            parents: action.parents,
            value: action.value,
            timestamp: action.timestamp,
            deleted: false,
        };

        field.insert(block);
    }

    pub fn delete(&mut self, action: DeleteParams) {
        let field = self
            .fields
            .entry(action.selector)
            .or_insert_with(BlockSet::new);

        field.delete(&action.parents);
    }

    pub fn to_map(&self) -> FxHashMap<Selector, &Value> {
        let mut map = FxHashMap::default();

        for (selector, field) in &self.fields {
            let latest_block = field.get_latest();

            if let Some(latest_block) = latest_block {
                map.insert(selector.clone(), &latest_block.value);
            }
        }

        map
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Selector, &Value)> {
        self.fields.iter().filter_map(|(selector, field)| {
            field.get_latest().map(|block| (selector, &block.value))
        })
    }
}
