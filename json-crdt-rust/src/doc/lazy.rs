use bytes::Bytes;

use crate::{
    serde::{BufferReader, Serializable, SerializationError},
    view::{ViewCache, ViewError},
    CachedObjectValue, DocError, GlobalClientId, ObjRef, Selector, Timestamp, Value,
};

use super::{
    full::{FullDoc, FullDocBuilder},
    traits::ReadableDoc,
};

pub struct LazyDoc {
    view: ViewCache,
    buffer: Bytes,
    builder: FullDocBuilder,
}

impl LazyDoc {
    pub fn load(
        client_id: GlobalClientId,
        timestamp: Timestamp,
        buffer: Bytes,
    ) -> Result<Self, DocError> {
        let reader = BufferReader::load(buffer.clone())?;
        let view = ViewCache::from_buffer(reader.view_cache())?;

        Ok(Self {
            view,
            buffer,
            builder: FullDocBuilder::new(client_id, timestamp, reader),
        })
    }

    pub fn prepare_full_doc_step(&mut self) -> Result<Option<FullDoc>, DocError> {
        self.builder.build_step()
    }
}

impl ReadableDoc for LazyDoc {
    fn get<TRef: Into<ObjRef>, TSelector: Into<Selector>>(
        &self,
        object_ref: TRef,
        selector: TSelector,
    ) -> Result<Option<&Value>, DocError> {
        let object_ref: ObjRef = object_ref.into();
        let selector: Selector = selector.into();

        Ok(self.view.get(object_ref, selector)?)
    }

    fn get_text<TRef: Into<ObjRef>>(&self, object_ref: TRef) -> Result<Option<String>, DocError> {
        let object_ref: ObjRef = object_ref.into();

        match self.view.get_object(object_ref)? {
            Some(CachedObjectValue::Text(value)) => Ok(Some(value.to_string())),
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

impl Serializable for LazyDoc {
    fn serialize(&self) -> Result<Vec<u8>, SerializationError> {
        // Because lazy docs are read-only, we can just pass the original buffer as serialization
        Ok(self.buffer.to_vec())
    }
}
