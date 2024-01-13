use bytes::Bytes;

use crate::{
    client_registry::{ClientRegistry, ClientRemappable},
    operation_log::OperationLog,
    serde::{serialize, BufferReader, BufferRegions, Serializable, SerializationError},
    transaction::Transaction,
    view::{View, ViewError},
    Doc, DocError, GlobalClientId, ObjRef, ObjectValue, Selector, Timestamp, Value,
};

use super::traits::{ReadableDoc, WritableDoc};

pub struct FullDoc {
    operation_log: OperationLog,
    view: View,
    client_registry: ClientRegistry,
}

impl FullDoc {
    pub fn new(client_id: GlobalClientId, timestamp: Timestamp) -> Self {
        let client_registry = ClientRegistry::new(client_id, timestamp);

        Self {
            operation_log: OperationLog::new(client_registry.get_current_id()),
            view: View::new(client_registry.get_current_id()),
            client_registry,
        }
    }

    pub fn from_buffer(
        client_id: GlobalClientId,
        timestamp: Timestamp,
        buffer: Bytes,
    ) -> Result<Self, DocError> {
        let mut reader = BufferReader::load(buffer)?;
        let mut builder = FullDocBuilder::new(client_id, timestamp, reader);

        loop {
            if let Some(doc) = builder.build_step()? {
                return Ok(doc);
            }
        }
    }

    fn from_components(
        client_id: GlobalClientId,
        timestamp: Timestamp,
        operation_log: OperationLog,
        view: View,
        client_registry: ClientRegistry,
    ) -> Self {
        Self {
            operation_log,
            view,
            client_registry,
        }
    }
}

impl ReadableDoc for FullDoc {
    fn get<TRef: Into<crate::ObjRef>, TSelector: Into<crate::Selector>>(
        &self,
        object: TRef,
        selector: TSelector,
    ) -> Result<Option<&Value>, DocError> {
        let object: ObjRef = object.into();
        let selector: Selector = selector.into();

        Ok(self.view.get(object, selector)?)
    }

    fn get_text<TRef: Into<crate::ObjRef>>(
        &self,
        object: TRef,
    ) -> Result<Option<String>, DocError> {
        let object: ObjRef = object.into();

        match self.view.get_object(object)? {
            Some(ObjectValue::Text(value)) => Ok(Some(value.to_string())),
            Some(_) => Err(DocError::ViewError(ViewError::IncompatibleTypes(
                "expected text".to_string(),
            ))),
            None => Ok(None),
        }
    }

    fn as_map<'a>(&'a self) -> Result<crate::DataMap<'a>, DocError> {
        Ok(self.view.as_map())
    }
}

impl WritableDoc for FullDoc {
    fn transaction(&mut self) -> Transaction {
        Transaction::new(
            &mut self.operation_log,
            &mut self.view,
            &mut self.client_registry,
        )
    }

    fn merge(&mut self, other: &Doc) -> Result<(), DocError> {
        let other_doc = other
            .handle
            .as_full()
            .ok_or_else(|| DocError::DocumentNotReady)?;

        let other_docs_clients = other_doc.client_registry.get_clients();
        let remappings = self.client_registry.register_clients(other_docs_clients);

        if let Some(remappings) = remappings {
            self.operation_log.remap_client_ids(&remappings);
            self.view
                .repopulate(&self.operation_log, &self.client_registry)?;
        }

        // TODO: make this actually efficient (from here and forward)
        let mut other_client_registry = other_doc.client_registry.clone();
        let other_remappings =
            other_client_registry.register_clients(self.client_registry.get_clients());

        for operation in other_doc.operation_log.iter_sorted() {
            let mut operation = operation.clone();

            if let Some(remappings) = &other_remappings {
                operation.remap_client_ids(remappings);
            }

            self.operation_log.apply_operation(operation)?;
        }

        self.view
            .repopulate(&self.operation_log, &self.client_registry)?;

        Ok(())
    }
}

impl Serializable for FullDoc {
    fn serialize(&self) -> Result<Vec<u8>, SerializationError> {
        let serialized = serialize(BufferRegions {
            client_registry: self.client_registry.serialize()?,
            operation_log: self.operation_log.serialize()?,
            view_cache: self.view.serialize()?,
        })?;

        Ok(serialized)
    }
}

pub struct FullDocBuilder {
    client_id: GlobalClientId,
    timestamp: Timestamp,
    reader: BufferReader,
}

impl FullDocBuilder {
    pub fn new(client_id: GlobalClientId, timestamp: Timestamp, reader: BufferReader) -> Self {
        Self {
            client_id,
            timestamp,
            reader,
        }
    }

    pub fn build_step(&mut self) -> Result<Option<FullDoc>, DocError> {
        // TODO: This method is intended to be refactored in the future to be incremental.
        //       model as a state machine and make each step divisible

        let (client_registry, remappings) = ClientRegistry::from_buffer(
            self.client_id.clone(),
            self.timestamp,
            self.reader.client_registry(),
        )?;

        let operation_log = OperationLog::from_buffer(
            client_registry.get_current_id(),
            remappings,
            &mut self.reader.operation_log(),
        )?;

        let mut view = View::new(client_registry.get_current_id());
        view.repopulate(&operation_log, &client_registry)?;

        let doc = FullDoc::from_components(
            self.client_id.clone(),
            self.timestamp,
            operation_log,
            view,
            client_registry,
        );

        Ok(Some(doc))
    }
}

// TODO: partial full doc object that takes the buffer (reference?) and load
// the view and oplog incrementally
