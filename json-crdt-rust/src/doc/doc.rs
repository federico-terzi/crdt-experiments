use crate::{
    client_registry::{ClientRegistry, ClientRegistryError, ClientRemappable, ClientRemappings},
    crdt::text::TextCRDT,
    operation_log::{OperationLog, OperationLogError},
    serde::{Serializable, SerializationError},
    transaction::Transaction,
    types::GlobalClientId,
    view::{View, ViewError},
    InsertTextAction, ObjRef, ObjectValue, Operation, OperationAction, OperationId, ScalarValue,
    Selector, SequenceBlockId, Timestamp, Value,
};
use bytes::Bytes;
use chrono::Utc;
use enum_as_inner::EnumAsInner;
use thiserror::Error;

use super::{
    full::FullDoc,
    lazy::LazyDoc,
    traits::{ReadableDoc, WritableDoc},
};

pub struct Doc {
    pub(crate) handle: DocHandle,
}

#[derive(EnumAsInner)]
pub(crate) enum DocHandle {
    Lazy(LazyDoc),
    Full(FullDoc),
}

pub enum DocStatus {
    Cached,
    Ready,
}

impl<'a> Doc {
    pub fn new(client_id: GlobalClientId) -> Self {
        let timestamp = Utc::now().timestamp_millis() as u64;
        Self::new_with_timestamp(client_id, timestamp)
    }

    pub fn new_with_timestamp(client_id: GlobalClientId, timestamp: Timestamp) -> Self {
        let doc = FullDoc::new(client_id, timestamp);
        let handle = DocHandle::Full(doc);
        Self { handle }
    }

    pub fn load(client_id: GlobalClientId, buffer: Bytes) -> Result<Self, DocError> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        Self::load_with_timestamp(client_id, timestamp, buffer)
    }

    pub fn load_with_timestamp(
        client_id: GlobalClientId,
        timestamp: Timestamp,
        buffer: Bytes,
    ) -> Result<Self, DocError> {
        let doc = FullDoc::from_buffer(client_id, timestamp, buffer)?;
        let handle = DocHandle::Full(doc);
        Ok(Self { handle })
    }

    pub fn lazy(client_id: GlobalClientId, buffer: Bytes) -> Result<Self, DocError> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        Self::lazy_with_timestamp(client_id, timestamp, buffer)
    }

    pub fn lazy_with_timestamp(
        client_id: GlobalClientId,
        timestamp: Timestamp,
        buffer: Bytes,
    ) -> Result<Self, DocError> {
        let doc = LazyDoc::load(client_id, timestamp, buffer)?;
        let handle = DocHandle::Lazy(doc);
        Ok(Self { handle })
    }

    pub fn status(&self) -> DocStatus {
        match &self.handle {
            DocHandle::Lazy(_) => DocStatus::Cached,
            DocHandle::Full(_) => DocStatus::Ready,
        }
    }

    pub fn initialize(&mut self) -> Result<bool, DocError> {
        Ok(self.initialize_step(u32::MAX)?)
    }

    pub fn initialize_step(&mut self, iterations: u32) -> Result<bool, DocError> {
        match &mut self.handle {
            // Already initialized
            DocHandle::Full(_) => Ok(true),

            // Not initialized, forward the query
            DocHandle::Lazy(doc) => {
                for _ in 0..iterations {
                    if let Some(full_doc) = doc.prepare_full_doc_step()? {
                        self.handle = DocHandle::Full(full_doc);
                        return Ok(true);
                    }
                }

                Ok(false)
            }
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, DocError> {
        match &self.handle {
            DocHandle::Lazy(doc) => Ok(doc.serialize()?),
            DocHandle::Full(doc) => Ok(doc.serialize()?),
        }
    }

    fn with_full_doc<T: 'a>(
        &'a mut self,
        action: impl FnOnce(&'a mut FullDoc) -> Result<T, DocError>,
    ) -> Result<T, DocError> {
        if self.handle.is_full() {
            let doc = self.handle.as_full_mut().expect("expected full doc");
            return action(doc);
        }

        // A write operation was requested, but document is still in "lazy" state.
        // Force an initialization to transform it into a full document.
        self.initialize()?;
        let handle = self
            .handle
            .as_full_mut()
            .expect("Doc should be initialized by now");
        action(handle)
    }
}

impl ReadableDoc for Doc {
    fn get<TRef: Into<ObjRef>, TSelector: Into<Selector>>(
        &self,
        object: TRef,
        selector: TSelector,
    ) -> Result<Option<&Value>, DocError> {
        match &self.handle {
            DocHandle::Lazy(doc) => doc.get(object, selector),
            DocHandle::Full(doc) => doc.get(object, selector),
        }
    }

    fn get_text<TRef: Into<ObjRef>>(&self, object: TRef) -> Result<Option<String>, DocError> {
        match &self.handle {
            DocHandle::Lazy(doc) => doc.get_text(object),
            DocHandle::Full(doc) => doc.get_text(object),
        }
    }

    fn as_map<'a>(&'a self) -> Result<crate::DataMap<'a>, DocError> {
        match &self.handle {
            DocHandle::Lazy(doc) => doc.as_map(),
            DocHandle::Full(doc) => doc.as_map(),
        }
    }
}

impl WritableDoc for Doc {
    fn transaction(&mut self) -> Transaction {
        self.with_full_doc(|doc| Ok(doc.transaction()))
            .expect("unable to create transaction")
    }

    fn merge(&mut self, other: &Self) -> Result<(), DocError> {
        self.with_full_doc(|doc| doc.merge(other))
    }
}

#[derive(Error, Debug)]
pub enum DocError {
    #[error("document not ready")]
    DocumentNotReady,

    #[error("serialization error: {0}")]
    SerializationError(#[from] SerializationError),

    #[error("client registry: {0}")]
    ClientRegistryError(#[from] ClientRegistryError),

    #[error("view error: {0}")]
    ViewError(#[from] ViewError),

    #[error("operation log error: {0}")]
    OperationLogError(#[from] OperationLogError),
}
