use std::{borrow::Cow, cmp::Ordering, collections::VecDeque};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use rustc_hash::FxHashMap;

use crate::{
    serde::{
        deserialize_obj_ref, deserialize_selector, deserialize_value, serialize_obj_ref,
        serialize_selector, serialize_value, Serializable, SerializationError,
    },
    CachedObjectValue, DataMap, DataMapValue, ObjRef, Selector, Value,
};

use super::{view::View, ViewError};

pub struct ViewCache {
    objects: FxHashMap<ObjRef, CachedObjectValue>,
}

impl<'a> ViewCache {
    pub fn from_buffer(buffer: Bytes) -> Result<Self, SerializationError> {
        let mut buffer = Bytes::from(buffer);
        let items_len = buffer
            .get_u32_varint()
            .map_err(|_| SerializationError::Malformed("unable to read items len".to_string()))?;

        let mut objects = FxHashMap::default();
        for _ in 0..items_len {
            let obj_ref = deserialize_obj_ref(&mut buffer)?;
            let object_value = deserialize_cached_value_object(&mut buffer)?;
            objects.insert(obj_ref, object_value);
        }

        Ok(Self { objects })
    }

    pub fn get_object(&self, object: ObjRef) -> Result<Option<&CachedObjectValue>, ViewError> {
        Ok(self.objects.get(&object))
    }

    pub fn get(&self, object: ObjRef, selector: Selector) -> Result<Option<&Value>, ViewError> {
        let map = self.get_object(object)?;
        match map {
            Some(CachedObjectValue::Map(map)) => Ok(map.get(&selector)),
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
            CachedObjectValue::Map(map) => {
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
            CachedObjectValue::Text(text) => DataMapValue::Text(Cow::Borrowed(text)),
        }
    }
}

impl From<&View> for ViewCache {
    fn from(view: &View) -> Self {
        let objects = view
            .objects
            .iter()
            .map(|(obj_ref, object_value)| (obj_ref.clone(), CachedObjectValue::from(object_value)))
            .collect();

        Self { objects }
    }
}

enum CachedObjectValueType {
    Map,
    Text,
}

impl From<CachedObjectValueType> for u8 {
    fn from(value: CachedObjectValueType) -> Self {
        match value {
            CachedObjectValueType::Map => 1,
            CachedObjectValueType::Text => 2,
        }
    }
}

impl From<u8> for CachedObjectValueType {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Map,
            2 => Self::Text,
            _ => panic!("invalid cached object value type"),
        }
    }
}

impl Serializable for ViewCache {
    fn serialize(&self) -> Result<Vec<u8>, crate::serde::SerializationError> {
        let mut sorted_keys: Vec<&ObjRef> = self.objects.keys().collect();
        sorted_keys.sort_by(|a, b| match (a, b) {
            (ObjRef::Root, ObjRef::Root) => Ordering::Equal,
            (ObjRef::Root, ObjRef::Object(_)) => Ordering::Less,
            (ObjRef::Object(_), ObjRef::Root) => Ordering::Greater,
            (ObjRef::Object(a), ObjRef::Object(b)) => {
                if a.client_id == b.client_id {
                    a.sequence.cmp(&b.sequence)
                } else {
                    a.client_id.cmp(&b.client_id)
                }
            }
        });

        let mut buf = BytesMut::new();

        let items_len: u32 = sorted_keys.len().try_into().expect("too many items");
        buf.put_u32_varint(items_len);

        for obj_ref in sorted_keys {
            serialize_obj_ref(obj_ref, &mut buf);
            let object_value = self.objects.get(obj_ref).expect("object not found");
            serialize_cached_object_value(object_value, &mut buf);
        }

        Ok(buf.to_vec())
    }
}

fn serialize_cached_object_value(value: &CachedObjectValue, buf: &mut BytesMut) {
    match value {
        CachedObjectValue::Map(map) => {
            buf.put_u8(CachedObjectValueType::Map.into());
            buf.put_u32_varint(map.len() as u32);
            for (selector, value) in map.iter() {
                serialize_selector(selector, buf);
                serialize_value(value, buf);
            }
        }
        CachedObjectValue::Text(text) => {
            buf.put_u8(CachedObjectValueType::Text.into());

            let text_len: u32 = text.len().try_into().expect("text too large");
            buf.put_u32_varint(text_len);
            buf.put_slice(text.as_bytes());
        }
    }
}

fn deserialize_cached_value_object(
    buf: &mut Bytes,
) -> Result<CachedObjectValue, SerializationError> {
    let value_type = buf.get_u8();
    let value_type: CachedObjectValueType = value_type.into();

    match value_type {
        CachedObjectValueType::Map => {
            let map_len = buf
                .get_u32_varint()
                .map_err(|_| SerializationError::Malformed("unable to read map len".to_string()))?;

            let mut map = FxHashMap::default();
            for _ in 0..map_len {
                let selector = deserialize_selector(buf)?;
                let value = deserialize_value(buf)?;
                map.insert(selector, value);
            }

            Ok(CachedObjectValue::Map(map))
        }
        CachedObjectValueType::Text => {
            let text_len = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read text len".to_string())
            })?;

            let text = buf.copy_to_bytes(text_len as usize);
            Ok(CachedObjectValue::Text(
                String::from_utf8(text.to_vec()).map_err(|_| {
                    SerializationError::Malformed("unable to read text".to_string())
                })?,
            ))
        }
    }
}
