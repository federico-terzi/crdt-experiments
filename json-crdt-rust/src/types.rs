use std::borrow::Cow;

use enum_as_inner::EnumAsInner;
use rustc_hash::FxHashMap;

use crate::{
    client_registry::{ClientRemappable, ClientRemappings},
    crdt::{map::map::MapCRDT, text::TextCRDT},
};

pub type GlobalClientId = String;
pub type ClientId = u32;
pub type Timestamp = u64;

#[derive(Clone)]
pub struct GlobalClient {
    pub created_at: Timestamp,
    pub global_id: GlobalClientId,
}

pub type SequenceIndex = u32;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct OperationId {
    pub client_id: ClientId,
    pub sequence: SequenceIndex,
}

impl ClientRemappable for OperationId {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        let new_client_id = mappings.get(&self.client_id).expect("client ID not found");
        self.client_id = *new_client_id;
    }
}

pub type ObjId = OperationId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjRef {
    Root,
    Object(ObjId),
}

impl ClientRemappable for ObjRef {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        match self {
            Self::Object(id) => id.remap_client_ids(mappings),
            _ => {}
        }
    }
}

impl From<ObjId> for ObjRef {
    fn from(id: ObjId) -> Self {
        Self::Object(id)
    }
}

impl From<&ObjRef> for ObjRef {
    fn from(obj: &ObjRef) -> Self {
        obj.clone()
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, EnumAsInner)]
pub enum Selector {
    Key(String),
    Index(usize),
}

impl From<&Selector> for Selector {
    fn from(value: &Selector) -> Self {
        value.clone()
    }
}

impl From<String> for Selector {
    fn from(key: String) -> Self {
        Self::Key(key)
    }
}

impl From<&str> for Selector {
    fn from(key: &str) -> Self {
        Self::Key(key.to_string())
    }
}

impl From<usize> for Selector {
    fn from(index: usize) -> Self {
        Self::Index(index)
    }
}

#[derive(Debug, EnumAsInner, Clone, PartialEq)]
pub enum ScalarValue {
    String(String),
    Int(i32),
    Double(f64),
    Bool(bool),
}

impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<bool> for ScalarValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i32> for ScalarValue {
    fn from(value: i32) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for ScalarValue {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

#[derive(Debug, EnumAsInner, Clone, PartialEq)]
pub enum ObjectValue {
    Map(MapCRDT),
    Text(TextCRDT),
}

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Value {
    Scalar(ScalarValue),
    Object(ObjRef),
}

impl ClientRemappable for Value {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        match self {
            Self::Object(obj) => obj.remap_client_ids(mappings),
            _ => {}
        }
    }
}

#[derive(Debug, EnumAsInner, Clone, PartialEq)]
pub enum CachedObjectValue {
    Map(FxHashMap<Selector, Value>),
    Text(String),
}

impl From<&ObjectValue> for CachedObjectValue {
    fn from(value: &ObjectValue) -> Self {
        match value {
            ObjectValue::Map(map) => {
                let mut cached_map = FxHashMap::default();
                for (key, value) in map.to_map() {
                    cached_map.insert(key, value.clone());
                }
                Self::Map(cached_map)
            }
            ObjectValue::Text(text) => Self::Text(text.to_string()),
        }
    }
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum DataMapValue<'a> {
    String(&'a str),
    Int(&'a i32),
    Double(&'a f64),
    Bool(&'a bool),
    Map(DataMap<'a>),
    Text(Cow<'a, str>),
}
pub type DataMap<'a> = FxHashMap<&'a Selector, DataMapValue<'a>>;

#[derive(Debug, Clone)]
pub struct Operation {
    pub id: OperationId,
    pub parent: Option<OperationId>,
    pub action: OperationAction,
    pub timestamp: Timestamp,
}

impl ClientRemappable for Operation {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        let new_client_id = mappings
            .get(&self.id.client_id)
            .expect("client ID not found");
        self.id.client_id = *new_client_id;

        if let Some(parent) = self.parent.as_mut() {
            let new_client_id = mappings
                .get(&parent.client_id)
                .expect("client ID not found");
            parent.client_id = *new_client_id;
        }

        self.action.remap_client_ids(mappings);
    }
}

#[derive(Debug, Clone)]
pub enum OperationAction {
    CreateMap(CreateMapAction),
    SetMapValue(SetMapValueAction),
    DeleteMapValue(DeleteMapValueAction),
    CreateText(CreateTextAction),
    InsertText(InsertTextAction),
    DeleteText(DeleteTextAction),
}

impl ClientRemappable for OperationAction {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        match self {
            Self::CreateMap(action) => action.remap_client_ids(mappings),
            Self::SetMapValue(action) => action.remap_client_ids(mappings),
            Self::DeleteMapValue(action) => action.remap_client_ids(mappings),
            Self::CreateText(action) => action.remap_client_ids(mappings),
            Self::InsertText(action) => action.remap_client_ids(mappings),
            Self::DeleteText(action) => action.remap_client_ids(mappings),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MapBlockId {
    pub client_id: ClientId,
    pub sequence: SequenceIndex,
}

impl ClientRemappable for MapBlockId {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        let new_client_id = mappings.get(&self.client_id).expect("client ID not found");
        self.client_id = *new_client_id;
    }
}

#[derive(Debug, Clone)]
pub struct CreateMapAction {
    pub object: ObjRef,
    pub selector: Selector,
    pub id: MapBlockId,
    pub parents: Vec<MapBlockId>,
}

impl ClientRemappable for CreateMapAction {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        self.object.remap_client_ids(mappings);
        self.id.remap_client_ids(mappings);
        for parent in &mut self.parents {
            parent.remap_client_ids(mappings);
        }
    }
}

#[derive(Debug, Clone)]
pub struct SetMapValueAction {
    pub object: ObjRef,
    pub selector: Selector,
    pub id: MapBlockId,
    pub parents: Vec<MapBlockId>,
    pub value: Value,
}

impl ClientRemappable for SetMapValueAction {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        self.object.remap_client_ids(mappings);
        self.id.remap_client_ids(mappings);
        for parent in &mut self.parents {
            parent.remap_client_ids(mappings);
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeleteMapValueAction {
    pub object: ObjRef,
    pub selector: Selector,
    pub parents: Vec<MapBlockId>,
}

impl ClientRemappable for DeleteMapValueAction {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        self.object.remap_client_ids(mappings);
        for parent in &mut self.parents {
            parent.remap_client_ids(mappings);
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateTextAction {
    pub object: ObjRef,
    pub selector: Selector,
    pub id: MapBlockId,
    pub parents: Vec<MapBlockId>,
}

impl ClientRemappable for CreateTextAction {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        self.object.remap_client_ids(mappings);
        self.id.remap_client_ids(mappings);
        for parent in &mut self.parents {
            parent.remap_client_ids(mappings);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SequenceBlockId {
    pub client_id: ClientId,
    pub sequence: SequenceIndex,
}

impl SequenceBlockId {
    pub fn new(client_id: ClientId, sequence: SequenceIndex) -> Self {
        Self {
            client_id,
            sequence,
        }
    }
}

impl ClientRemappable for SequenceBlockId {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        let new_client_id = mappings.get(&self.client_id).expect("client ID not found");
        self.client_id = *new_client_id;
    }
}

#[derive(Debug, Clone)]
pub struct InsertTextAction {
    pub object: ObjRef,
    pub id: SequenceBlockId,
    pub value: String,
    pub left: Option<SequenceBlockId>,
}

impl ClientRemappable for InsertTextAction {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        self.object.remap_client_ids(mappings);
        self.id.remap_client_ids(mappings);
        if let Some(left) = self.left.as_mut() {
            left.remap_client_ids(mappings);
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeleteTextAction {
    pub object: ObjRef,
    pub left: SequenceBlockId,
    pub right: SequenceBlockId,
}

impl ClientRemappable for DeleteTextAction {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings) {
        self.object.remap_client_ids(mappings);
        self.left.remap_client_ids(mappings);
        self.right.remap_client_ids(mappings);
    }
}
