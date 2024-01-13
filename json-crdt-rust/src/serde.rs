use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_varint::{VarIntError, VarIntSupport, VarIntSupportMut};
use thiserror::Error;

use crate::{ObjRef, Value};

pub trait Serializable {
    fn serialize(&self) -> Result<Vec<u8>, SerializationError>;
}

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("malformed buffer {0}")]
    Malformed(String),
}

pub struct BufferRegions {
    pub view_cache: Vec<u8>,
    pub client_registry: Vec<u8>,
    pub operation_log: Vec<u8>,
}

pub fn serialize(regions: BufferRegions) -> Result<Vec<u8>, SerializationError> {
    let mut buffer = BytesMut::new();

    let view_cache_len: u32 = regions
        .view_cache
        .len()
        .try_into()
        .expect("view cache too large");
    buffer.put_u32_varint(view_cache_len);
    buffer.put_slice(&regions.view_cache);

    let client_registry_len: u32 = regions
        .client_registry
        .len()
        .try_into()
        .expect("client registry too large");
    buffer.put_u32_varint(client_registry_len);
    buffer.put_slice(&regions.client_registry);

    let operation_log_len: u32 = regions
        .operation_log
        .len()
        .try_into()
        .expect("operation log too large");
    buffer.put_u32_varint(operation_log_len);
    buffer.put_slice(&regions.operation_log);

    Ok(buffer.to_vec())
}

pub struct BufferReader {
    view_cache: Bytes,
    client_registry: Bytes,
    operation_log: Bytes,
}

impl<'a> BufferReader {
    pub fn load(buffer: Bytes) -> Result<Self, SerializationError> {
        let mut buffer = Bytes::from(buffer);
        let view_cache_len = buffer.get_u32_varint().map_err(|_| {
            SerializationError::Malformed("unable to read view_cache len".to_string())
        })?;
        let view_cache_bytes = buffer.copy_to_bytes(view_cache_len as usize);

        let client_registry_len = buffer.get_u32_varint().map_err(|_| {
            SerializationError::Malformed("unable to read client_registry len".to_string())
        })?;
        let client_registry_bytes = buffer.copy_to_bytes(client_registry_len as usize);

        let operation_log_len = buffer.get_u32_varint().map_err(|_| {
            SerializationError::Malformed("unable to read operation_log len".to_string())
        })?;
        let operation_log_bytes = buffer.copy_to_bytes(operation_log_len as usize);

        Ok(Self {
            view_cache: view_cache_bytes,
            client_registry: client_registry_bytes,
            operation_log: operation_log_bytes,
        })
    }

    pub fn view_cache(&'a self) -> Bytes {
        self.view_cache.clone()
    }

    pub fn client_registry(&'a self) -> Bytes {
        self.client_registry.clone()
    }

    pub fn operation_log(&'a self) -> Bytes {
        self.operation_log.clone()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum ObjRefType {
    Root,
    Object,
}

impl From<u8> for ObjRefType {
    fn from(value: u8) -> Self {
        match value {
            0 => ObjRefType::Root,
            1 => ObjRefType::Object,
            _ => panic!("unknown object reference type: {}", value),
        }
    }
}

impl From<&ObjRefType> for u8 {
    fn from(value: &ObjRefType) -> Self {
        match value {
            ObjRefType::Root => 0,
            ObjRefType::Object => 1,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum SelectorType {
    Key,
    Index,
}

impl From<u8> for SelectorType {
    fn from(value: u8) -> Self {
        match value {
            0 => SelectorType::Key,
            1 => SelectorType::Index,
            _ => panic!("unknown selector type: {}", value),
        }
    }
}

impl From<&SelectorType> for u8 {
    fn from(value: &SelectorType) -> Self {
        match value {
            SelectorType::Key => 0,
            SelectorType::Index => 1,
        }
    }
}

pub fn serialize_obj_ref(obj_ref: &crate::ObjRef, buf: &mut BytesMut) {
    // TODO: probably we can use client_id 0 to mean "root"
    match obj_ref {
        crate::ObjRef::Root => {
            buf.put_u8((&ObjRefType::Root).into());
        }
        crate::ObjRef::Object(obj_ref) => {
            buf.put_u8((&ObjRefType::Object).into());
            buf.put_u32_varint(obj_ref.client_id);
            buf.put_u32_varint(obj_ref.sequence);
        }
    }
}

pub fn deserialize_obj_ref(buf: &mut Bytes) -> Result<ObjRef, SerializationError> {
    let obj_ref_type = buf
        .get_u8()
        .try_into()
        .map_err(|_| SerializationError::Malformed("unable to read obj_ref type".to_string()))?;

    match obj_ref_type {
        ObjRefType::Root => Ok(ObjRef::Root),
        ObjRefType::Object => {
            let client_id = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read client_id".to_string())
            })?;
            let sequence = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read sequence".to_string())
            })?;
            Ok(ObjRef::Object(crate::ObjId {
                client_id,
                sequence,
            }))
        }
    }
}

pub fn serialize_selector(selector: &crate::Selector, buf: &mut BytesMut) {
    match selector {
        crate::Selector::Index(index) => {
            buf.put_u8((&SelectorType::Index).into());
            let index: u32 = (*index).try_into().expect("index too large");
            buf.put_u32_varint(index);
        }
        crate::Selector::Key(key) => {
            buf.put_u8((&SelectorType::Key).into());
            let key_len: u32 = key.len().try_into().expect("key too large");
            buf.put_u32_varint(key_len);
            buf.put_slice(key.as_bytes());
        }
    }
}

pub fn deserialize_selector(buf: &mut Bytes) -> Result<crate::Selector, SerializationError> {
    let selector_type = buf
        .get_u8()
        .try_into()
        .map_err(|_| SerializationError::Malformed("unable to read selector type".to_string()))?;

    match selector_type {
        SelectorType::Index => {
            let index = buf
                .get_u32_varint()
                .map_err(|_| SerializationError::Malformed("unable to read index".to_string()))?;
            Ok(crate::Selector::Index(index as usize))
        }
        SelectorType::Key => {
            let key_len = buf
                .get_u32_varint()
                .map_err(|_| SerializationError::Malformed("unable to read key len".to_string()))?;
            let key = buf.copy_to_bytes(key_len as usize);
            Ok(crate::Selector::Key(
                String::from_utf8(key.to_vec())
                    .map_err(|_| SerializationError::Malformed("unable to read key".to_string()))?,
            ))
        }
    }
}

enum ValueType {
    String,
    Int,
    Double,
    Bool,
    Object,
}

impl From<ValueType> for u8 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::String => 1,
            ValueType::Int => 2,
            ValueType::Double => 3,
            ValueType::Bool => 4,
            ValueType::Object => 5,
        }
    }
}

impl From<u8> for ValueType {
    fn from(value: u8) -> Self {
        match value {
            1 => ValueType::String,
            2 => ValueType::Int,
            3 => ValueType::Double,
            4 => ValueType::Bool,
            5 => ValueType::Object,
            _ => panic!("unknown value type: {}", value),
        }
    }
}

// TODO: remove?
pub fn serialize_value(value: &Value, buf: &mut BytesMut) {
    match value {
        Value::Scalar(scalar) => match scalar {
            crate::ScalarValue::String(string) => {
                buf.put_u8(ValueType::String.into());
                let string_len: u32 = string.len().try_into().expect("string too large");
                buf.put_u32_varint(string_len);
                buf.put_slice(string.as_bytes());
            }
            crate::ScalarValue::Int(int) => {
                buf.put_u8(ValueType::Int.into());
                buf.put_i32_varint(*int);
            }
            crate::ScalarValue::Double(double) => {
                buf.put_u8(ValueType::Double.into());
                buf.put_f64(*double);
            }
            crate::ScalarValue::Bool(bool) => {
                // TODO: we can optimize this by having a separate type for BOOL true and false
                buf.put_u8(ValueType::Bool.into());
                buf.put_u8(if *bool { 1 } else { 0 });
            }
        },
        Value::Object(object) => {
            buf.put_u8(ValueType::Object.into());
            serialize_obj_ref(object, buf);
        }
    }
}

pub fn deserialize_value(buf: &mut Bytes) -> Result<Value, SerializationError> {
    let value_type = buf
        .get_u8()
        .try_into()
        .map_err(|_| SerializationError::Malformed("unable to read value type".to_string()))?;

    match value_type {
        ValueType::String => {
            let string_len = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read string len".to_string())
            })?;
            let string = buf.copy_to_bytes(string_len as usize);
            Ok(Value::Scalar(crate::ScalarValue::String(
                String::from_utf8(string.to_vec()).map_err(|_| {
                    SerializationError::Malformed("unable to read string".to_string())
                })?,
            )))
        }
        ValueType::Int => {
            let int = buf
                .get_i32_varint()
                .map_err(|_| SerializationError::Malformed("unable to read int".to_string()))?;
            Ok(Value::Scalar(crate::ScalarValue::Int(int)))
        }
        ValueType::Double => {
            let double = buf.get_f64();
            Ok(Value::Scalar(crate::ScalarValue::Double(double)))
        }
        ValueType::Bool => {
            let bool = buf.get_u8();
            Ok(Value::Scalar(crate::ScalarValue::Bool(bool != 0)))
        }
        ValueType::Object => {
            let obj_ref = deserialize_obj_ref(buf)?;
            Ok(Value::Object(obj_ref))
        }
    }
}
