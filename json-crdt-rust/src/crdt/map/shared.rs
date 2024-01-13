use crate::{MapBlockId, Timestamp, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct MapBlock {
    pub id: MapBlockId,
    pub parents: Vec<MapBlockId>,
    pub value: Value,
    pub timestamp: Timestamp,
    pub deleted: bool,
}
