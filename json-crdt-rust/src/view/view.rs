use std::borrow::Cow;

use rustc_hash::FxHashMap;
use thiserror::Error;

use crate::{
    client_registry::ClientRegistry,
    crdt::{
        map::map::{DeleteParams, MapCRDT, SetParams},
        text::TextCRDT,
    },
    operation_log::OperationLog,
    serde::Serializable,
    ClientId, DataMap, DataMapValue, ObjRef, ObjectValue, Operation, OperationAction, Selector,
    Value,
};

use super::ViewCache;

pub struct View {
    pub(crate) objects: FxHashMap<ObjRef, ObjectValue>,
}

impl<'a> View {
    pub fn new(client_id: ClientId) -> Self {
        let mut objects = FxHashMap::default();
        objects.insert(ObjRef::Root, ObjectValue::Map(MapCRDT::new(client_id)));

        Self { objects }
    }

    pub fn get_object<TRef: Into<ObjRef>>(
        &self,
        object: TRef,
    ) -> Result<Option<&ObjectValue>, ViewError> {
        let obj_ref: &ObjRef = &object.into();
        let object_value = self.objects.get(&obj_ref);
        Ok(object_value)
    }

    pub fn get_object_mut<TRef: Into<ObjRef>>(
        &mut self,
        object: TRef,
    ) -> Result<Option<&mut ObjectValue>, ViewError> {
        let obj_ref: &ObjRef = &object.into();
        let object_value = self.objects.get_mut(&obj_ref);
        Ok(object_value)
    }

    pub fn get(&self, object: ObjRef, selector: Selector) -> Result<Option<&Value>, ViewError> {
        let map = self.get_object(object)?;
        match map {
            Some(ObjectValue::Map(map)) => Ok(map.get(&selector)),
            Some(val) => Err(ViewError::IncompatibleTypes(format!(
                "expected map, found: {:?}",
                val
            ))),
            None => Ok(None),
        }
    }

    pub fn as_map(&'a self) -> DataMap<'a> {
        self.as_map_recursive(&ObjRef::Root)
            .into_map()
            .expect("expected root to be a map")
    }

    fn as_map_recursive(&'a self, obj_ref: &ObjRef) -> DataMapValue {
        let obj = self.objects.get(&obj_ref).expect("object not found");
        match obj {
            ObjectValue::Map(map) => {
                let mut data_map: DataMap = DataMap::default();
                for (selector, value) in map.iter() {
                    let data_map_value: DataMapValue<'a> = match value {
                        Value::Scalar(scalar) => match scalar {
                            crate::ScalarValue::String(string) => DataMapValue::String(string),
                            crate::ScalarValue::Int(int) => DataMapValue::Int(int),
                            crate::ScalarValue::Double(double) => DataMapValue::Double(double),
                            crate::ScalarValue::Bool(bool) => DataMapValue::Bool(bool),
                        },
                        Value::Object(obj_ref) => self.as_map_recursive(&obj_ref),
                    };
                    data_map.insert(selector, data_map_value);
                }
                DataMapValue::Map(data_map)
            }
            ObjectValue::Text(text) => DataMapValue::Text(Cow::Owned(text.to_string())),
        }
    }

    pub fn apply_local_operation(
        &mut self,
        operation: &Operation,
        client_registry: &ClientRegistry,
    ) -> Result<(), ViewError> {
        self.execute_operation(&operation, client_registry)
    }

    pub fn repopulate(
        &mut self,
        log: &OperationLog,
        client_registry: &ClientRegistry,
    ) -> Result<(), ViewError> {
        // TODO: make this actually efficient
        // TODO: if log sequence is still compatible with view history, just execute the latest operations
        // TODO: if log sequence is NOT compatible with view history, recompute the whole view

        self.objects.clear();
        self.objects.insert(
            ObjRef::Root,
            ObjectValue::Map(MapCRDT::new(client_registry.get_current_id())),
        );
        for operation in log.iter() {
            self.execute_operation(operation, client_registry)?;
        }

        Ok(())
    }

    fn execute_operation(
        &mut self,
        operation: &Operation,
        client_registry: &ClientRegistry,
    ) -> Result<(), ViewError> {
        match &operation.action {
            OperationAction::CreateMap(action) => {
                let obj_ref = ObjRef::from(operation.id);
                self.objects.insert(
                    obj_ref.clone(),
                    ObjectValue::Map(MapCRDT::new(client_registry.get_current_id())),
                );

                let map = self.get_map_mut(&action.object)?;
                map.set(SetParams {
                    selector: action.selector.clone(),
                    id: action.id.clone(),
                    parents: action.parents.clone(),
                    timestamp: operation.timestamp,
                    value: Value::Object(obj_ref),
                })
            }
            OperationAction::SetMapValue(action) => {
                let map = self.get_map_mut(&action.object)?;
                map.set(SetParams {
                    selector: action.selector.clone(),
                    id: action.id.clone(),
                    parents: action.parents.clone(),
                    timestamp: operation.timestamp,
                    value: action.value.clone(),
                });
            }
            OperationAction::DeleteMapValue(action) => {
                let map = self.get_map_mut(&action.object)?;
                map.delete(DeleteParams {
                    selector: action.selector.clone(),
                    parents: action.parents.clone(),
                });
            }
            OperationAction::CreateText(action) => {
                let obj_ref = ObjRef::from(operation.id);
                self.objects.insert(
                    obj_ref.clone(),
                    ObjectValue::Text(TextCRDT::new(client_registry.get_current_id())),
                );

                let map = self.get_map_mut(&action.object)?;
                map.set(SetParams {
                    selector: action.selector.clone(),
                    id: action.id.clone(),
                    parents: action.parents.clone(),
                    timestamp: operation.timestamp,
                    value: Value::Object(obj_ref),
                })
            }
            OperationAction::InsertText(action) => {
                let obj = self.get_object_mut(&action.object)?;
                match obj {
                    Some(ObjectValue::Text(text)) => text.insert(&action),
                    // TODO: handle better! What should happen in this case?
                    _ => {}
                }
            }
            OperationAction::DeleteText(action) => {
                let obj = self.get_object_mut(&action.object)?;
                match obj {
                    Some(ObjectValue::Text(text)) => text.delete(&action),
                    // TODO: handle better! What should happen in this case?
                    _ => {}
                }
            }
            _ => {
                unimplemented!("operation action not implemented");
            }
        }

        Ok(())
    }

    fn get_map_mut(&mut self, object: &ObjRef) -> Result<&mut MapCRDT, ViewError> {
        let object_value = self.objects.get_mut(object);
        match object_value {
            Some(ObjectValue::Map(map)) => Ok(map),
            Some(val) => Err(ViewError::IncompatibleTypes(format!(
                "expected map, found: {:?}",
                val
            ))),
            None => Err(ViewError::InconsistentHierarchy(format!(
                "object {:?} not found",
                object
            ))),
        }
    }
}

#[derive(Error, Debug)]
pub enum ViewError {
    #[error("inconsistent hierarchy: {0}")]
    InconsistentHierarchy(String),

    #[error("incompatible types: {0}")]
    IncompatibleTypes(String),

    #[error("bad operation: {0}")]
    BadOperation(String),
}

impl Serializable for View {
    fn serialize(&self) -> Result<Vec<u8>, crate::serde::SerializationError> {
        let cache: ViewCache = self.into();
        cache.serialize()
    }
}
