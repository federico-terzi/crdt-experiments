use crate::{
    client_registry::{self, ClientRegistry},
    operation_log::{OperationLog, OperationLogError},
    view::{View, ViewError},
    CreateMapAction, CreateTextAction, DeleteMapValueAction, DeleteTextAction, InsertTextAction,
    ObjRef, ObjectValue, Operation, OperationAction, OperationId, ScalarValue, Selector,
    SetMapValueAction, Value,
};
use chrono::Utc;
use thiserror::Error;

pub struct Transaction<'a> {
    op_log: &'a mut OperationLog,
    view: &'a mut View,
    client_registry: &'a mut ClientRegistry,
}

impl<'a> Transaction<'a> {
    pub fn new(
        op_log: &'a mut OperationLog,
        view: &'a mut View,
        client_registry: &'a mut ClientRegistry,
    ) -> Self {
        Self {
            op_log,
            view,
            client_registry,
        }
    }

    pub fn set_scalar<TRef: Into<ObjRef>, TSelector: Into<Selector>, TValue: Into<ScalarValue>>(
        &mut self,
        obj: TRef,
        sel: TSelector,
        value: TValue,
    ) -> Result<(), TransactionError> {
        let obj: ObjRef = obj.into();
        let sel: Selector = sel.into();
        let value: ScalarValue = value.into();

        let map = self.view.get_object_mut(&obj)?;
        let (block_id, block_parents) = match map {
            Some(ObjectValue::Map(map)) => {
                let map_id = map.next_id();
                let parents = map.get_latest_ids(&sel);
                (map_id, parents)
            }
            actual_value => {
                return Err(TransactionError::IncompatibleTypes(format!(
                    "expected map, found: {:?}",
                    actual_value
                )))
            }
        };

        self.create_action(|_self| {
            Ok(OperationAction::SetMapValue(SetMapValueAction {
                object: obj,
                selector: sel,
                id: block_id,
                parents: block_parents,
                value: Value::Scalar(value),
            }))
        })?;

        Ok(())
    }

    pub fn delete<TRef: Into<ObjRef>, TSelector: Into<Selector>>(
        &mut self,
        obj: TRef,
        sel: TSelector,
    ) -> Result<(), TransactionError> {
        let obj: ObjRef = obj.into();
        let sel: Selector = sel.into();

        let map = self.view.get_object_mut(&obj)?;
        let block_parents = match map {
            Some(ObjectValue::Map(map)) => {
                let parents = map.get_latest_ids(&sel);
                parents
            }
            actual_value => {
                return Err(TransactionError::IncompatibleTypes(format!(
                    "expected map, found: {:?}",
                    actual_value
                )))
            }
        };

        self.create_action(|_self| {
            Ok(OperationAction::DeleteMapValue(DeleteMapValueAction {
                object: obj,
                selector: sel,
                parents: block_parents,
            }))
        })?;

        Ok(())
    }

    pub fn create_map<TRef: Into<ObjRef>, TSelector: Into<Selector>>(
        &mut self,
        obj: TRef,
        sel: TSelector,
    ) -> Result<ObjRef, TransactionError> {
        let obj: ObjRef = obj.into();
        let sel: Selector = sel.into();

        let map = self.view.get_object_mut(&obj)?;
        let (block_id, block_parents) = match map {
            Some(ObjectValue::Map(map)) => {
                let map_id = map.next_id();
                let parents = map.get_latest_ids(&sel);
                (map_id, parents)
            }
            actual_value => {
                return Err(TransactionError::IncompatibleTypes(format!(
                    "expected map, found: {:?}",
                    actual_value
                )))
            }
        };

        let obj_id = self.create_action(|_self| {
            Ok(OperationAction::CreateMap(CreateMapAction {
                object: obj,
                selector: sel,
                id: block_id,
                parents: block_parents,
            }))
        })?;

        Ok(ObjRef::Object(obj_id))
    }

    pub fn create_text<TRef: Into<ObjRef>, TSelector: Into<Selector>>(
        &mut self,
        obj: TRef,
        sel: TSelector,
    ) -> Result<ObjRef, TransactionError> {
        let obj: ObjRef = obj.into();
        let sel: Selector = sel.into();

        let map = self.view.get_object_mut(&obj)?;
        let (block_id, block_parents) = match map {
            Some(ObjectValue::Map(map)) => {
                let map_id = map.next_id();
                let parents = map.get_latest_ids(&sel);
                (map_id, parents)
            }
            actual_value => {
                return Err(TransactionError::IncompatibleTypes(format!(
                    "expected map, found: {:?}",
                    actual_value
                )))
            }
        };

        let text_id = self.create_action(|_self| {
            Ok(OperationAction::CreateText(CreateTextAction {
                object: obj,
                selector: sel,
                id: block_id,
                parents: block_parents,
            }))
        })?;

        Ok(ObjRef::Object(text_id))
    }

    pub fn get_text<TRef: Into<ObjRef>, TSelector: Into<Selector>>(
        &mut self,
        obj: TRef,
        sel: TSelector,
    ) -> Result<Option<ObjRef>, TransactionError> {
        let obj: ObjRef = obj.into();
        let sel: Selector = sel.into();

        let view_value = self.view.get(obj, sel)?;
        match view_value {
            Some(Value::Object(obj_ref)) => match self.view.get_object(obj_ref)? {
                Some(ObjectValue::Text(_)) => Ok(Some(obj_ref.clone())),
                Some(_) => {
                    return Err(TransactionError::IncompatibleTypes(format!(
                        "expected text, found: {:?}",
                        view_value
                    )))
                }
                None => panic!("expected text object to be present"),
            },
            Some(_) => {
                return Err(TransactionError::IncompatibleTypes(format!(
                    "expected object, found: {:?}",
                    view_value
                )))
            }
            None => Ok(None),
        }
    }

    pub fn get_or_create_text<TRef: Into<ObjRef>, TSelector: Into<Selector>>(
        &mut self,
        obj: TRef,
        sel: TSelector,
    ) -> Result<ObjRef, TransactionError> {
        let obj: ObjRef = obj.into();
        let sel: Selector = sel.into();

        match self.get_text(&obj, &sel)? {
            Some(obj_ref) => {
                return Ok(obj_ref);
            }
            None => self.create_text(obj, sel),
        }
    }

    pub fn append_text<TRef: Into<ObjRef>, TValue: Into<String>>(
        &mut self,
        obj: TRef,
        value: TValue,
    ) -> Result<(), TransactionError> {
        let obj: ObjRef = obj.into();
        let value: String = value.into();

        let view_value = self.view.get_object_mut(&obj)?;
        let (text_block_id, left) = match view_value {
            Some(crate::ObjectValue::Text(text)) => {
                let text_block_id = text.next_id(
                    value
                        .len()
                        .try_into()
                        .map_err(|_| TransactionError::TextTooLong)?,
                );

                let left = text.last_block();
                (text_block_id, left)
            }
            actual_value => {
                return Err(TransactionError::IncompatibleTypes(format!(
                    "expected text, found: {:?}",
                    actual_value
                )))
            }
        };

        self.create_action(|_self| {
            Ok(OperationAction::InsertText(InsertTextAction {
                object: obj,
                id: text_block_id,
                value,
                left,
            }))
        })?;

        Ok(())
    }

    pub fn insert_text<TRef: Into<ObjRef>, TValue: Into<String>>(
        &mut self,
        obj: TRef,
        index: u32,
        value: TValue,
    ) -> Result<(), TransactionError> {
        let obj: ObjRef = obj.into();
        let value: String = value.into();

        let view_value = self.view.get_object_mut(&obj)?;
        let (text_block_id, left) = match view_value {
            Some(crate::ObjectValue::Text(text)) => {
                let text_block_id = text.next_id(
                    value
                        .len()
                        .try_into()
                        .map_err(|_| TransactionError::TextTooLong)?,
                );

                let left = text.find_block_ending_at(index);
                (text_block_id, left)
            }
            actual_value => {
                return Err(TransactionError::IncompatibleTypes(format!(
                    "expected text, found: {:?}",
                    actual_value
                )))
            }
        };

        self.create_action(|_self| {
            Ok(OperationAction::InsertText(InsertTextAction {
                object: obj,
                id: text_block_id,
                value,
                left,
            }))
        })?;

        Ok(())
    }

    pub fn delete_text<TRef: Into<ObjRef>>(
        &mut self,
        obj: TRef,
        index: u32,
        count: u32,
    ) -> Result<(), TransactionError> {
        let obj: ObjRef = obj.into();

        let view_value = self.view.get_object_mut(&obj)?;
        let (left, right) = match view_value {
            Some(crate::ObjectValue::Text(text)) => {
                let left = text.find_block_starting_at(index);
                let right = text.find_block_ending_at(index + count);
                (left, right)
            }
            actual_value => {
                return Err(TransactionError::IncompatibleTypes(format!(
                    "expected text, found: {:?}",
                    actual_value
                )))
            }
        };

        let left = left.ok_or_else(|| TransactionError::InvalidIndex("left".to_string()))?;
        let right = right.ok_or_else(|| TransactionError::InvalidIndex("right".to_string()))?;

        self.create_action(|_self| {
            Ok(OperationAction::DeleteText(DeleteTextAction {
                object: obj,
                left,
                right,
            }))
        })?;

        Ok(())
    }

    pub fn commit(self) -> Result<(), TransactionError> {
        // TODO: here rollback all the previous actions and pack them into a single operation if possible
        // let compacted_actions = Self::compact_actions(self.actions_buffer);
        // let operation = self
        //     .op_log
        //     .register_actions(&self.client_registry, compacted_actions)?;
        // self.view
        //     .apply_local_operation(operation, self.object_registry)?;

        Ok(())
    }

    fn create_action(
        &mut self,
        callback: impl FnOnce(&mut Self) -> Result<OperationAction, TransactionError>,
    ) -> Result<OperationId, TransactionError> {
        let action = callback(self)?;

        // TODO: this might need to be provided from outside
        let timestamp = Utc::now().timestamp_millis() as u64;
        let operation = self.op_log.apply_local_action(action, timestamp)?;
        self.view
            .apply_local_operation(operation, &self.client_registry)?;

        Ok(operation.id)
    }
}

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("operation log error: {0}")]
    OperationLogError(#[from] OperationLogError),

    #[error("incompatible types: {0}")]
    IncompatibleTypes(String),

    #[error("text too long")]
    TextTooLong,

    #[error("invalid index: {0}")]
    InvalidIndex(String),

    #[error("view error: {0}")]
    ViewError(#[from] ViewError),
}
