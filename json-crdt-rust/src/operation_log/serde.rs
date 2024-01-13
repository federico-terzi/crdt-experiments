use std::{
    cmp::Ordering,
    ops::{Add, AddAssign},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use num_integer::Integer;

use crate::{
    serde::{
        serialize_obj_ref, serialize_selector, serialize_value, ObjRefType, SelectorType,
        SerializationError,
    },
    ClientId, ObjId, ObjRef, Operation, OperationAction, OperationId, Selector, SequenceBlockId,
    SequenceIndex, Timestamp, Value,
};

pub fn serialize_operations<'a>(
    operations: impl Iterator<Item = &'a Operation>,
) -> Result<Vec<u8>, SerializationError> {
    let mut buf = BytesMut::new();

    let mut sorted_operations: Vec<&Operation> = operations.collect();
    sorted_operations.sort_by(compare_operations);

    let sorted_operations_len: u32 = sorted_operations
        .len()
        .try_into()
        .expect("too many operations");
    buf.put_u32_varint(sorted_operations_len);

    let mut columns = Columns::default();

    for operation in sorted_operations {
        populate_columns_for_operation(operation, &mut columns);
    }

    columns.serialize(&mut buf);

    Ok(buf.to_vec())
}

pub fn deserialize_operations(bytes: &mut Bytes) -> Result<Vec<Operation>, SerializationError> {
    let operations_len: u32 = bytes.get_u32_varint().map_err(|_| {
        SerializationError::Malformed("unable to read operations length".to_string())
    })?;

    let mut columns = Columns::deserialize(bytes)?;

    let mut operations = Vec::new();

    for _ in 0..operations_len {
        let operation = parse_operation_from_columns(&mut columns)?;
        operations.push(operation);
    }

    Ok(operations)
}

// TODO: move to the top-level serde module?
trait SerializableType: Sized + PartialEq + std::fmt::Debug + Clone {
    fn serialize(&self, buf: &mut BytesMut);
    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError>;
}

impl SerializableType for ClientId {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u32_varint(*self);
    }

    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        Ok(buf
            .get_u32_varint()
            .map_err(|_| SerializationError::Malformed("unable to read client ID".to_string()))?)
    }
}

impl SerializableType for bool {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u8(*self as u8);
    }

    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        let value = buf.get_u8();
        Ok(value != 0)
    }
}

impl SerializableType for Timestamp {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u64_varint(*self);
    }

    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        Ok(buf
            .get_u64_varint()
            .map_err(|_| SerializationError::Malformed("unable to read timestamp".to_string()))?)
    }
}

impl SerializableType for u8 {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u8(*self);
    }

    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        Ok(buf.get_u8())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SerializedValueType {
    String,
    Int,
    Double,
    Bool,
    Object,
}

impl From<u8> for SerializedValueType {
    fn from(value: u8) -> Self {
        match value {
            1 => SerializedValueType::String,
            2 => SerializedValueType::Int,
            3 => SerializedValueType::Double,
            4 => SerializedValueType::Bool,
            5 => SerializedValueType::Object,
            _ => panic!("unknown value type: {}", value),
        }
    }
}

impl SerializableType for Value {
    fn serialize(&self, buf: &mut BytesMut) {
        match self {
            Value::Scalar(scalar) => match scalar {
                crate::ScalarValue::String(string) => {
                    buf.put_u8(SerializedValueType::String as u8);
                    let string_len: u32 = string.len().try_into().expect("string too large");
                    buf.put_u32_varint(string_len);
                    buf.put_slice(string.as_bytes());
                }
                crate::ScalarValue::Int(int) => {
                    buf.put_u8(SerializedValueType::Int as u8);
                    buf.put_i32_varint(*int);
                }
                crate::ScalarValue::Double(double) => {
                    buf.put_u8(SerializedValueType::Double as u8);
                    buf.put_f64(*double);
                }
                crate::ScalarValue::Bool(bool) => {
                    // TODO: we can optimize this by having a separate type for BOOL true and false
                    buf.put_u8(SerializedValueType::Bool as u8);
                    buf.put_u8(if *bool { 1 } else { 0 });
                }
            },
            Value::Object(object) => {
                buf.put_u8(SerializedValueType::Object as u8);
                serialize_obj_ref(object, buf);
            }
        }
    }

    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        todo!()
    }
}

trait CompressionStrategy<Type>: Default {
    fn serialize(&self, buf: &mut BytesMut, values: &[Type]);
    fn deserialize(&self, buf: &mut Bytes) -> Result<Vec<Type>, SerializationError>;
}

#[derive(Default)]
struct NoneCompressionStrategy {}

impl<Type: SerializableType> CompressionStrategy<Type> for NoneCompressionStrategy {
    fn serialize(&self, buf: &mut BytesMut, values: &[Type]) {
        let values_len: u32 = values.len().try_into().expect("too many values");
        buf.put_u32_varint(values_len);

        for value in values {
            value.serialize(buf);
        }
    }

    fn deserialize(&self, buf: &mut Bytes) -> Result<Vec<Type>, SerializationError> {
        let values_len: u32 = buf.get_u32_varint().map_err(|_| {
            SerializationError::Malformed("unable to read values length".to_string())
        })?;

        let mut values = Vec::new();

        for _ in 0..values_len {
            let value = Type::deserialize(buf)?;
            values.push(value);
        }

        Ok(values)
    }
}
#[derive(Default)]
struct DuplicateCompressionStrategy {}

#[derive(Debug)]
struct DuplicateRange<'a, Type: SerializableType> {
    value: &'a Type,
    count: u32,
}

impl DuplicateCompressionStrategy {
    fn compress<Type: SerializableType>(values: &[Type]) -> Vec<DuplicateRange<'_, Type>> {
        let mut ranges = Vec::new();

        let mut prev_value: Option<&Type> = None;
        let mut count = 0;

        for (index, value) in values.iter().enumerate() {
            let is_last = index == values.len() - 1;
            match prev_value {
                Some(prev_value) => {
                    if value == prev_value {
                        count += 1;
                    } else {
                        ranges.push(DuplicateRange {
                            value: prev_value,
                            count,
                        });
                        count = 1;
                    }
                }
                None => {
                    count = 1;
                }
            }

            if is_last {
                ranges.push(DuplicateRange { value, count });
            }

            prev_value = Some(value);
        }

        ranges
    }
}

impl<Type: SerializableType> CompressionStrategy<Type> for DuplicateCompressionStrategy {
    fn serialize(&self, buf: &mut BytesMut, values: &[Type]) {
        let ranges = Self::compress(values);
        // println!("duplicate ranges: {:?}", ranges);

        let ranges_len: u32 = ranges.len().try_into().expect("too many ranges");
        buf.put_u32_varint(ranges_len);

        for range in ranges {
            range.value.serialize(buf);
            buf.put_u32_varint(range.count);
        }
    }

    fn deserialize(&self, buf: &mut Bytes) -> Result<Vec<Type>, SerializationError> {
        let ranges_len: u32 = buf.get_u32_varint().map_err(|_| {
            SerializationError::Malformed("unable to read ranges length".to_string())
        })?;

        let mut values = Vec::new();

        for _ in 0..ranges_len {
            let value = Type::deserialize(buf)?;
            let count = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read range count".to_string())
            })?;

            for _ in 0..count {
                values.push(value.clone());
            }
        }

        Ok(values)
    }
}

#[derive(Default)]
struct SequenceCompressionStrategy {}

#[derive(Debug, Clone)]
struct SequenceRange {
    start: u32,
    count: u32,
}

impl SequenceCompressionStrategy {
    fn compress(values: &[u32]) -> Vec<SequenceRange> {
        let mut ranges = Vec::new();

        let mut prev_value: Option<&u32> = None;
        let mut current_range = SequenceRange { start: 0, count: 0 };

        for (index, value) in values.iter().enumerate() {
            let is_last = index == values.len() - 1;
            let is_sequential = if let Some(previous) = prev_value {
                value == &(previous + 1)
            } else {
                false
            };

            if is_sequential {
                current_range.count += 1;
            } else {
                if prev_value.is_some() {
                    ranges.push(current_range.clone());
                }
                current_range.count = 1;
                current_range.start = *value;
            }

            if is_last {
                ranges.push(current_range.clone());
            }

            prev_value = Some(value);
        }

        ranges
    }
}

impl CompressionStrategy<u32> for SequenceCompressionStrategy {
    fn serialize(&self, buf: &mut BytesMut, values: &[u32]) {
        let ranges = Self::compress(values);
        // println!("sequence ranges: {:?}", ranges);

        let ranges_len: u32 = ranges.len().try_into().expect("too many ranges");
        buf.put_u32_varint(ranges_len);

        for range in ranges {
            range.start.serialize(buf);
            buf.put_u32_varint(range.count);
        }
    }

    fn deserialize(&self, buf: &mut Bytes) -> Result<Vec<u32>, SerializationError> {
        let ranges_len: u32 = buf.get_u32_varint().map_err(|_| {
            SerializationError::Malformed("unable to read ranges length".to_string())
        })?;

        let mut values = Vec::new();

        for _ in 0..ranges_len {
            let start = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read range start".to_string())
            })?;
            let count = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read range count".to_string())
            })?;

            for offset in 0..count {
                let actual_value = start + offset;
                values.push(actual_value);
            }
        }

        Ok(values)
    }
}

#[derive(Default)]
struct TwoWaySequenceCompressionStrategy {}

#[derive(Debug, Clone, PartialEq)]
enum TwoWaySequenceRangeDirection {
    Increasing,
    Decreasing,
}

impl From<u8> for TwoWaySequenceRangeDirection {
    fn from(value: u8) -> Self {
        match value {
            0 => TwoWaySequenceRangeDirection::Increasing,
            1 => TwoWaySequenceRangeDirection::Decreasing,
            _ => panic!("unknown two way sequence range direction: {}", value),
        }
    }
}

impl From<TwoWaySequenceRangeDirection> for u8 {
    fn from(value: TwoWaySequenceRangeDirection) -> Self {
        match value {
            TwoWaySequenceRangeDirection::Increasing => 0,
            TwoWaySequenceRangeDirection::Decreasing => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct TwoWaySequenceRange {
    direction: TwoWaySequenceRangeDirection,
    start: u32,
    count: u32,
}

// TODO: test
impl TwoWaySequenceCompressionStrategy {
    fn compress(values: &[u32]) -> Vec<TwoWaySequenceRange> {
        let mut ranges = Vec::new();

        let mut prev_value: Option<&u32> = None;

        let mut current_direction: Option<TwoWaySequenceRangeDirection> = None;
        let mut current_count = 0;
        let mut current_start = 0;

        for (index, value) in values.iter().enumerate() {
            let is_last = index == values.len() - 1;

            let direction = if let Some(previous) = prev_value {
                if value == &(previous + 1) {
                    Some(TwoWaySequenceRangeDirection::Increasing)
                } else if value == &(previous - 1) {
                    Some(TwoWaySequenceRangeDirection::Decreasing)
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(direction) = direction {
                if let Some(current_direction_ref) = current_direction.as_ref() {
                    if direction == *current_direction_ref {
                        current_count += 1;
                    } else {
                        if prev_value.is_some() {
                            ranges.push(TwoWaySequenceRange {
                                direction: current_direction_ref.clone(),
                                count: current_count,
                                start: current_start,
                            });
                        }
                        current_direction = Some(direction);
                        current_count = 1;
                        current_start = *value;
                    }
                } else {
                    current_direction = Some(direction);
                    current_count += 1;
                }
            } else {
                if prev_value.is_some() {
                    ranges.push(TwoWaySequenceRange {
                        direction: current_direction
                            .clone()
                            .unwrap_or(TwoWaySequenceRangeDirection::Increasing),
                        count: current_count,
                        start: current_start,
                    });
                }
                current_direction = None;
                current_count = 1;
                current_start = *value;
            }

            if is_last {
                ranges.push(TwoWaySequenceRange {
                    direction: current_direction
                        .clone()
                        .unwrap_or(TwoWaySequenceRangeDirection::Increasing),
                    count: current_count,
                    start: current_start,
                });
            }

            prev_value = Some(value);
        }

        ranges
    }
}

impl CompressionStrategy<u32> for TwoWaySequenceCompressionStrategy {
    fn serialize(&self, buf: &mut BytesMut, values: &[u32]) {
        let ranges = Self::compress(values);
        // println!("two way sequence ranges: {:?}", ranges);

        let ranges_len: u32 = ranges.len().try_into().expect("too many ranges");
        buf.put_u32_varint(ranges_len);

        for range in ranges {
            // Basic but more inefficient way
            buf.put_u8(range.direction.into());
            range.start.serialize(buf);
            buf.put_u32_varint(range.count);

            // More efficient way, enable if we need to save space
            // range.start.serialize(buf);
            // // We steal the first bit of the count to store the direction
            // let direction: u8 = range.direction.into();
            // let merged_count: u32 = (range.count << 1) | direction as u32;
            // buf.put_u32_varint(merged_count);
        }
    }

    fn deserialize(&self, buf: &mut Bytes) -> Result<Vec<u32>, SerializationError> {
        let ranges_len: u32 = buf.get_u32_varint().map_err(|_| {
            SerializationError::Malformed("unable to read ranges length".to_string())
        })?;

        let mut values = Vec::new();

        for _ in 0..ranges_len {
            let direction: TwoWaySequenceRangeDirection = buf.get_u8().into();
            let start = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read range start".to_string())
            })?;
            let count = buf.get_u32_varint().map_err(|_| {
                SerializationError::Malformed("unable to read range count".to_string())
            })?;

            for offset in 0..count {
                let actual_value = match direction {
                    TwoWaySequenceRangeDirection::Increasing => start + offset,
                    TwoWaySequenceRangeDirection::Decreasing => start - offset,
                };
                values.push(actual_value);
            }
        }

        Ok(values)
    }
}

#[derive(Default)]
struct DeltaCompressionStrategy {}

impl DeltaCompressionStrategy {
    fn calculate_deltas<Type: Integer + Copy>(values: &[Type]) -> Vec<Type> {
        let mut deltas = Vec::new();

        let mut prev_value: Option<&Type> = None;
        for value in values {
            match prev_value {
                Some(prev_value) => {
                    let delta = value.sub(*prev_value);
                    deltas.push(delta);
                }
                None => {
                    deltas.push(*value);
                }
            }

            prev_value = Some(value);
        }

        deltas
    }
}

impl<Type: SerializableType + Integer + Copy + Add + Default> CompressionStrategy<Type>
    for DeltaCompressionStrategy
{
    fn serialize(&self, buf: &mut BytesMut, values: &[Type]) {
        let deltas = Self::calculate_deltas(values);
        // println!("deltas: {:?}", deltas);

        let deltas_len: u32 = deltas.len().try_into().expect("too many ranges");
        buf.put_u32_varint(deltas_len);

        for delta in deltas {
            delta.serialize(buf);
        }
    }

    fn deserialize(&self, buf: &mut Bytes) -> Result<Vec<Type>, SerializationError> {
        let deltas_len: u32 = buf.get_u32_varint().map_err(|_| {
            SerializationError::Malformed("unable to read deltas length".to_string())
        })?;

        let mut values = Vec::new();
        let mut previous: Type = Type::default();

        for _ in 0..deltas_len {
            let delta = Type::deserialize(buf)?;
            let value = previous.add(delta);
            previous = value;
            values.push(value);
        }

        Ok(values)
    }
}

struct Column<Type, Strategy: CompressionStrategy<Type>> {
    cursor: usize,
    values: Vec<Type>,
    strategy: Strategy,
}

impl<Type, Strategy: CompressionStrategy<Type>> Column<Type, Strategy> {
    fn push(&mut self, value: Type) {
        self.values.push(value);
    }

    fn read(&mut self) -> Result<&Type, SerializationError> {
        if self.cursor >= self.values.len() {
            return Err(SerializationError::Malformed(
                "column read out of bounds".to_string(),
            ));
        }

        let value = &self.values[self.cursor];
        self.cursor += 1;
        Ok(value)
    }

    fn read_multiple(&mut self, count: usize) -> Result<&[Type], SerializationError> {
        if self.cursor + count > self.values.len() {
            return Err(SerializationError::Malformed(
                "column read out of bounds".to_string(),
            ));
        }

        let start = self.cursor;
        let end = start + count;
        self.cursor += count;
        Ok(&self.values[start..end])
    }

    fn serialize(&self, buf: &mut BytesMut) {
        self.strategy.serialize(buf, &self.values);
    }

    fn deserialize(&mut self, buf: &mut Bytes) -> Result<(), SerializationError> {
        self.values = self.strategy.deserialize(buf)?;
        Ok(())
    }
}

impl<Strategy: CompressionStrategy<u8>> Column<u8, Strategy> {
    fn read_str(&mut self, len: usize) -> Result<&str, SerializationError> {
        let bytes = self.read_multiple(len)?;
        let string = std::str::from_utf8(bytes)
            .map_err(|_| SerializationError::Malformed("unable to read string".to_string()))?;
        Ok(string)
    }
}

impl<Strategy: CompressionStrategy<u8>> Column<u8, Strategy> {
    fn push_str(&mut self, string: &str) {
        let bytes = string.as_bytes();
        for byte in bytes {
            self.values.push(*byte);
        }
    }
}

impl<Type, Strategy: CompressionStrategy<Type>> Default for Column<Type, Strategy> {
    fn default() -> Self {
        Self {
            cursor: 0,
            values: Default::default(),
            strategy: Default::default(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum SerializedAction {
    CreateMap,
    SetMapValue,
    DeleteMapValue,
    CreateText,
    InsertText,
    DeleteText,
}

impl From<u8> for SerializedAction {
    fn from(value: u8) -> Self {
        match value {
            1 => SerializedAction::CreateMap,
            2 => SerializedAction::SetMapValue,
            3 => SerializedAction::DeleteMapValue,
            4 => SerializedAction::CreateText,
            5 => SerializedAction::InsertText,
            6 => SerializedAction::DeleteText,
            _ => panic!("unknown action type: {}", value),
        }
    }
}

impl From<&SerializedAction> for u8 {
    fn from(value: &SerializedAction) -> Self {
        match value {
            SerializedAction::CreateMap => 1,
            SerializedAction::SetMapValue => 2,
            SerializedAction::DeleteMapValue => 3,
            SerializedAction::CreateText => 4,
            SerializedAction::InsertText => 5,
            SerializedAction::DeleteText => 6,
        }
    }
}

impl SerializableType for SerializedAction {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u8(self.into());
    }

    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        let value = buf.get_u8();
        Ok(value.into())
    }
}

impl SerializableType for ObjRefType {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u8(self.into());
    }

    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        let value = buf.get_u8();
        Ok(value.into())
    }
}

impl SerializableType for SelectorType {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u8(self.into());
    }

    fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        let value = buf.get_u8();
        Ok(value.into())
    }
}

#[derive(Default)]
struct Columns {
    op_id_client_id: Column<ClientId, DuplicateCompressionStrategy>,
    op_id_sequence: Column<SequenceIndex, SequenceCompressionStrategy>,

    op_has_parent: Column<bool, DuplicateCompressionStrategy>,
    op_parent_client_id: Column<ClientId, DuplicateCompressionStrategy>,
    op_parent_sequence: Column<SequenceIndex, SequenceCompressionStrategy>,

    op_timestamp: Column<Timestamp, DeltaCompressionStrategy>,

    op_action_type: Column<SerializedAction, DuplicateCompressionStrategy>,

    op_action_object_ref_type: Column<ObjRefType, DuplicateCompressionStrategy>,
    op_action_object_ref_client_id: Column<ClientId, DuplicateCompressionStrategy>,
    op_action_object_ref_sequence: Column<SequenceIndex, DuplicateCompressionStrategy>,

    op_action_selector_type: Column<SelectorType, DuplicateCompressionStrategy>,
    op_action_selector_key_len: Column<u32, DuplicateCompressionStrategy>,
    op_action_selector_key: Column<u8, NoneCompressionStrategy>,
    op_action_selector_indexes: Column<u32, DuplicateCompressionStrategy>,

    op_action_map_block_id_client_id: Column<ClientId, DuplicateCompressionStrategy>,
    op_action_map_block_id_sequence: Column<SequenceIndex, SequenceCompressionStrategy>,

    op_action_map_parents_len: Column<u32, DuplicateCompressionStrategy>,
    op_action_map_parents_client_id: Column<ClientId, DuplicateCompressionStrategy>,
    op_action_map_parents_sequence: Column<SequenceIndex, SequenceCompressionStrategy>,

    op_action_map_value: Column<Value, NoneCompressionStrategy>,

    op_action_sequence_block_id_client_id: Column<ClientId, DuplicateCompressionStrategy>,
    op_action_sequence_block_id_sequence: Column<SequenceIndex, SequenceCompressionStrategy>,

    op_action_text_value_len: Column<u32, DuplicateCompressionStrategy>,
    op_action_text_value: Column<u8, NoneCompressionStrategy>,

    op_action_has_left: Column<bool, DuplicateCompressionStrategy>,
    op_action_left_client_id: Column<ClientId, DuplicateCompressionStrategy>,
    op_action_left_sequence: Column<SequenceIndex, TwoWaySequenceCompressionStrategy>,

    op_action_right_client_id: Column<ClientId, DuplicateCompressionStrategy>,
    op_action_right_sequence: Column<SequenceIndex, TwoWaySequenceCompressionStrategy>,
}

impl Columns {
    pub fn serialize(&self, buf: &mut BytesMut) {
        self.op_id_client_id.serialize(buf);
        self.op_id_sequence.serialize(buf);
        self.op_has_parent.serialize(buf);
        self.op_parent_client_id.serialize(buf);
        self.op_parent_sequence.serialize(buf);
        self.op_timestamp.serialize(buf);
        self.op_action_type.serialize(buf);
        self.op_action_object_ref_type.serialize(buf);
        self.op_action_object_ref_client_id.serialize(buf);
        self.op_action_object_ref_sequence.serialize(buf);
        self.op_action_selector_type.serialize(buf);
        self.op_action_selector_key_len.serialize(buf);
        self.op_action_selector_key.serialize(buf);
        self.op_action_selector_indexes.serialize(buf);
        self.op_action_map_block_id_client_id.serialize(buf);
        self.op_action_map_block_id_sequence.serialize(buf);
        self.op_action_map_parents_len.serialize(buf);
        self.op_action_map_parents_client_id.serialize(buf);
        self.op_action_map_parents_sequence.serialize(buf);
        self.op_action_map_value.serialize(buf);
        self.op_action_sequence_block_id_client_id.serialize(buf);
        self.op_action_sequence_block_id_sequence.serialize(buf);
        self.op_action_text_value_len.serialize(buf);
        self.op_action_text_value.serialize(buf);
        self.op_action_has_left.serialize(buf);
        self.op_action_left_client_id.serialize(buf);
        self.op_action_left_sequence.serialize(buf);
        self.op_action_right_client_id.serialize(buf);
        self.op_action_right_sequence.serialize(buf);

        // TODO: add a check to make sure all fields have been serialized?
    }

    pub fn deserialize(buf: &mut Bytes) -> Result<Self, SerializationError> {
        let mut column = Self::default();

        column.op_id_client_id.deserialize(buf)?;
        column.op_id_sequence.deserialize(buf)?;
        column.op_has_parent.deserialize(buf)?;
        column.op_parent_client_id.deserialize(buf)?;
        column.op_parent_sequence.deserialize(buf)?;
        column.op_timestamp.deserialize(buf)?;
        column.op_action_type.deserialize(buf)?;
        column.op_action_object_ref_type.deserialize(buf)?;
        column.op_action_object_ref_client_id.deserialize(buf)?;
        column.op_action_object_ref_sequence.deserialize(buf)?;
        column.op_action_selector_type.deserialize(buf)?;
        column.op_action_selector_key_len.deserialize(buf)?;
        column.op_action_selector_key.deserialize(buf)?;
        column.op_action_selector_indexes.deserialize(buf)?;
        column.op_action_map_block_id_client_id.deserialize(buf)?;
        column.op_action_map_block_id_sequence.deserialize(buf)?;
        column.op_action_map_parents_len.deserialize(buf)?;
        column.op_action_map_parents_client_id.deserialize(buf)?;
        column.op_action_map_parents_sequence.deserialize(buf)?;
        column.op_action_map_value.deserialize(buf)?;
        column
            .op_action_sequence_block_id_client_id
            .deserialize(buf)?;
        column
            .op_action_sequence_block_id_sequence
            .deserialize(buf)?;
        column.op_action_text_value_len.deserialize(buf)?;
        column.op_action_text_value.deserialize(buf)?;
        column.op_action_has_left.deserialize(buf)?;
        column.op_action_left_client_id.deserialize(buf)?;
        column.op_action_left_sequence.deserialize(buf)?;
        column.op_action_right_client_id.deserialize(buf)?;
        column.op_action_right_sequence.deserialize(buf)?;

        Ok(column)
    }
}

fn populate_columns_for_operation(operation: &Operation, columns: &mut Columns) {
    columns.op_id_client_id.push(operation.id.client_id);
    columns.op_id_sequence.push(operation.id.sequence);

    if let Some(parent) = &operation.parent {
        columns.op_has_parent.push(true);
        columns.op_parent_client_id.push(parent.client_id);
        columns.op_parent_sequence.push(parent.sequence);
    } else {
        columns.op_has_parent.push(false);
    }

    columns.op_timestamp.push(operation.timestamp);

    populate_columns_for_action(&operation.action, columns);
}

fn parse_operation_from_columns(columns: &mut Columns) -> Result<Operation, SerializationError> {
    let id = OperationId {
        client_id: *columns.op_id_client_id.read()?,
        sequence: *columns.op_id_sequence.read()?,
    };

    let parent = if *columns.op_has_parent.read()? {
        Some(OperationId {
            client_id: *columns.op_parent_client_id.read()?,
            sequence: *columns.op_parent_sequence.read()?,
        })
    } else {
        None
    };

    let timestamp = *columns.op_timestamp.read()?;
    let action = parse_action_from_columns(columns)?;

    Ok(Operation {
        id,
        parent,
        timestamp,
        action,
    })
}

fn populate_columns_for_action(action: &OperationAction, columns: &mut Columns) {
    match action {
        OperationAction::CreateMap(action) => {
            populate_columns_for_create_map_action(action, columns);
        }
        OperationAction::SetMapValue(action) => {
            populate_columns_for_set_map_value_action(action, columns);
        }
        OperationAction::DeleteMapValue(action) => {
            populate_columns_for_delete_map_value_action(action, columns);
        }
        OperationAction::CreateText(action) => {
            populate_columns_for_create_text_action(action, columns);
        }
        OperationAction::InsertText(action) => {
            populate_columns_for_insert_text_action(action, columns);
        }
        OperationAction::DeleteText(action) => {
            populate_columns_for_delete_text_action(action, columns);
        }
    }
}

fn parse_action_from_columns(columns: &mut Columns) -> Result<OperationAction, SerializationError> {
    let action_type = columns.op_action_type.read()?;

    match action_type {
        SerializedAction::CreateMap => parse_create_map_action_from_columns(columns),
        SerializedAction::SetMapValue => parse_set_map_value_action_from_columns(columns),
        SerializedAction::DeleteMapValue => parse_delete_map_value_action_from_columns(columns),
        SerializedAction::CreateText => parse_create_text_action_from_columns(columns),
        SerializedAction::InsertText => parse_insert_text_action_from_columns(columns),
        SerializedAction::DeleteText => parse_delete_text_action_from_columns(columns),
    }
}

fn populate_columns_for_obj_ref(obj_ref: &crate::ObjRef, columns: &mut Columns) {
    match obj_ref {
        crate::ObjRef::Root => columns.op_action_object_ref_type.push(ObjRefType::Root),
        crate::ObjRef::Object(obj_ref) => {
            columns.op_action_object_ref_type.push(ObjRefType::Object);
            columns
                .op_action_object_ref_client_id
                .push(obj_ref.client_id);
            columns.op_action_object_ref_sequence.push(obj_ref.sequence);
        }
    }
}

fn parse_obj_ref_from_columns(columns: &mut Columns) -> Result<ObjRef, SerializationError> {
    let obj_ref_type = columns.op_action_object_ref_type.read()?;

    match obj_ref_type {
        ObjRefType::Root => Ok(ObjRef::Root),
        ObjRefType::Object => {
            let client_id = *columns.op_action_object_ref_client_id.read()?;
            let sequence = *columns.op_action_object_ref_sequence.read()?;
            Ok(ObjRef::Object(ObjId {
                client_id,
                sequence,
            }))
        }
    }
}

fn populate_columns_for_selector(selector: &Selector, columns: &mut Columns) {
    match selector {
        Selector::Key(key) => {
            columns.op_action_selector_type.push(SelectorType::Key);
            let key_len: u32 = key.len().try_into().expect("key too large");
            columns.op_action_selector_key_len.push(key_len);
            columns.op_action_selector_key.push_str(key);
        }
        Selector::Index(index) => {
            columns.op_action_selector_type.push(SelectorType::Index);
            let index_u32: u32 = (*index).try_into().expect("index too large");
            columns.op_action_selector_indexes.push(index_u32);
        }
    }
}

fn parse_selector_from_columns(columns: &mut Columns) -> Result<Selector, SerializationError> {
    let selector_type = columns.op_action_selector_type.read()?;

    match selector_type {
        SelectorType::Key => {
            let key_len: u32 = *columns.op_action_selector_key_len.read()?;
            let key_len_usize: usize = key_len
                .try_into()
                .map_err(|_| SerializationError::Malformed("key too long".to_string()))?;
            let key = columns.op_action_selector_key.read_str(key_len_usize)?;
            Ok(Selector::Key(key.to_string()))
        }
        SelectorType::Index => {
            let index_u32: u32 = *columns.op_action_selector_indexes.read()?;
            let index = index_u32
                .try_into()
                .map_err(|_| SerializationError::Malformed("index too large".to_string()))?;
            Ok(Selector::Index(index))
        }
    }
}

fn populate_columns_for_map_block_id(id: &crate::MapBlockId, columns: &mut Columns) {
    columns.op_action_map_block_id_client_id.push(id.client_id);
    columns.op_action_map_block_id_sequence.push(id.sequence);
}

fn parse_map_block_id_from_columns(
    columns: &mut Columns,
) -> Result<crate::MapBlockId, SerializationError> {
    let client_id = *columns.op_action_map_block_id_client_id.read()?;
    let sequence = *columns.op_action_map_block_id_sequence.read()?;
    Ok(crate::MapBlockId {
        client_id,
        sequence,
    })
}

fn populate_columns_for_create_map_action(action: &crate::CreateMapAction, columns: &mut Columns) {
    columns.op_action_type.push(SerializedAction::CreateMap);

    populate_columns_for_obj_ref(&action.object, columns);
    populate_columns_for_selector(&action.selector, columns);
    populate_columns_for_map_block_id(&action.id, columns);

    let parents_len: u32 = action.parents.len().try_into().expect("too many parents");
    columns.op_action_map_parents_len.push(parents_len);

    for parent in &action.parents {
        populate_columns_for_map_block_id(parent, columns);
    }
}

fn parse_create_map_action_from_columns(
    columns: &mut Columns,
) -> Result<OperationAction, SerializationError> {
    let obj_ref = parse_obj_ref_from_columns(columns)?;
    let selector = parse_selector_from_columns(columns)?;
    let id = parse_map_block_id_from_columns(columns)?;

    let parents_len: u32 = *columns.op_action_map_parents_len.read()?;
    let mut parents = Vec::new();

    for _ in 0..parents_len {
        let parent = parse_map_block_id_from_columns(columns)?;
        parents.push(parent);
    }

    Ok(OperationAction::CreateMap(crate::CreateMapAction {
        object: obj_ref,
        selector,
        id,
        parents,
    }))
}

fn populate_columns_for_set_map_value_action(
    action: &crate::SetMapValueAction,
    columns: &mut Columns,
) {
    columns.op_action_type.push(SerializedAction::SetMapValue);

    populate_columns_for_obj_ref(&action.object, columns);
    populate_columns_for_selector(&action.selector, columns);
    populate_columns_for_map_block_id(&action.id, columns);

    let parents_len: u32 = action.parents.len().try_into().expect("too many parents");
    columns.op_action_map_parents_len.push(parents_len);

    for parent in &action.parents {
        populate_columns_for_map_block_id(parent, columns);
    }

    columns.op_action_map_value.push(action.value.clone());
}

fn parse_set_map_value_action_from_columns(
    columns: &mut Columns,
) -> Result<OperationAction, SerializationError> {
    let obj_ref = parse_obj_ref_from_columns(columns)?;
    let selector = parse_selector_from_columns(columns)?;
    let id = parse_map_block_id_from_columns(columns)?;

    let parents_len: u32 = *columns.op_action_map_parents_len.read()?;
    let mut parents = Vec::new();

    for _ in 0..parents_len {
        let parent = parse_map_block_id_from_columns(columns)?;
        parents.push(parent);
    }

    let value = columns.op_action_map_value.read()?.clone();

    Ok(OperationAction::SetMapValue(crate::SetMapValueAction {
        object: obj_ref,
        selector,
        id,
        parents,
        value,
    }))
}

fn populate_columns_for_delete_map_value_action(
    action: &crate::DeleteMapValueAction,
    columns: &mut Columns,
) {
    columns
        .op_action_type
        .push(SerializedAction::DeleteMapValue);

    populate_columns_for_obj_ref(&action.object, columns);
    populate_columns_for_selector(&action.selector, columns);

    let parents_len: u32 = action.parents.len().try_into().expect("too many parents");
    columns.op_action_map_parents_len.push(parents_len);

    for parent in &action.parents {
        populate_columns_for_map_block_id(parent, columns);
    }
}

fn parse_delete_map_value_action_from_columns(
    columns: &mut Columns,
) -> Result<OperationAction, SerializationError> {
    let obj_ref = parse_obj_ref_from_columns(columns)?;
    let selector = parse_selector_from_columns(columns)?;

    let parents_len: u32 = *columns.op_action_map_parents_len.read()?;

    let mut parents = Vec::new();
    for _ in 0..parents_len {
        let parent = parse_map_block_id_from_columns(columns)?;
        parents.push(parent);
    }

    Ok(OperationAction::DeleteMapValue(
        crate::DeleteMapValueAction {
            object: obj_ref,
            selector,
            parents,
        },
    ))
}

fn populate_columns_for_create_text_action(
    action: &crate::CreateTextAction,
    columns: &mut Columns,
) {
    columns.op_action_type.push(SerializedAction::CreateText);

    populate_columns_for_obj_ref(&action.object, columns);
    populate_columns_for_selector(&action.selector, columns);
    populate_columns_for_map_block_id(&action.id, columns);

    let parents_len: u32 = action.parents.len().try_into().expect("too many parents");
    columns.op_action_map_parents_len.push(parents_len);

    for parent in &action.parents {
        populate_columns_for_map_block_id(parent, columns);
    }
}

fn parse_create_text_action_from_columns(
    columns: &mut Columns,
) -> Result<OperationAction, SerializationError> {
    let obj_ref = parse_obj_ref_from_columns(columns)?;
    let selector = parse_selector_from_columns(columns)?;
    let id = parse_map_block_id_from_columns(columns)?;

    let parents_len: u32 = *columns.op_action_map_parents_len.read()?;
    let mut parents = Vec::new();

    for _ in 0..parents_len {
        let parent = parse_map_block_id_from_columns(columns)?;
        parents.push(parent);
    }

    Ok(OperationAction::CreateText(crate::CreateTextAction {
        object: obj_ref,
        selector,
        id,
        parents,
    }))
}

fn populate_columns_for_sequence_block_id(id: &crate::SequenceBlockId, columns: &mut Columns) {
    columns
        .op_action_sequence_block_id_client_id
        .push(id.client_id);
    columns
        .op_action_sequence_block_id_sequence
        .push(id.sequence);
}

fn parse_sequence_block_id_from_columns(
    columns: &mut Columns,
) -> Result<crate::SequenceBlockId, SerializationError> {
    let client_id = *columns.op_action_sequence_block_id_client_id.read()?;
    let sequence = *columns.op_action_sequence_block_id_sequence.read()?;
    Ok(crate::SequenceBlockId {
        client_id,
        sequence,
    })
}

fn populate_columns_for_insert_text_action(
    action: &crate::InsertTextAction,
    columns: &mut Columns,
) {
    columns.op_action_type.push(SerializedAction::InsertText);

    populate_columns_for_obj_ref(&action.object, columns);
    populate_columns_for_sequence_block_id(&action.id, columns);

    // TODO: we can probably optimize this by avoiding the string clone
    let text_len: u32 = action.value.len().try_into().expect("text too long");
    columns.op_action_text_value_len.push(text_len);
    columns.op_action_text_value.push_str(&action.value);

    match action.left.as_ref() {
        Some(left) => {
            columns.op_action_has_left.push(true);
            columns.op_action_left_client_id.push(left.client_id);
            columns.op_action_left_sequence.push(left.sequence);
        }
        None => {
            columns.op_action_has_left.push(false);
        }
    }
}

fn parse_insert_text_action_from_columns(
    columns: &mut Columns,
) -> Result<OperationAction, SerializationError> {
    let obj_ref = parse_obj_ref_from_columns(columns)?;
    let id = parse_sequence_block_id_from_columns(columns)?;

    let text_len: u32 = *columns.op_action_text_value_len.read()?;
    let text_len_usize: usize = text_len
        .try_into()
        .map_err(|_| SerializationError::Malformed("text too long".to_string()))?;
    let text = columns
        .op_action_text_value
        .read_str(text_len_usize)?
        .to_string();

    let left = if *columns.op_action_has_left.read()? {
        let left_client_id = *columns.op_action_left_client_id.read()?;
        let left_sequence = *columns.op_action_left_sequence.read()?;
        Some(SequenceBlockId {
            client_id: left_client_id,
            sequence: left_sequence,
        })
    } else {
        None
    };

    Ok(OperationAction::InsertText(crate::InsertTextAction {
        object: obj_ref,
        id,
        value: text,
        left,
    }))
}

fn populate_columns_for_delete_text_action(
    action: &crate::DeleteTextAction,
    columns: &mut Columns,
) {
    columns.op_action_type.push(SerializedAction::DeleteText);

    populate_columns_for_obj_ref(&action.object, columns);
    columns.op_action_left_client_id.push(action.left.client_id);
    columns.op_action_left_sequence.push(action.left.sequence);
    columns
        .op_action_right_client_id
        .push(action.right.client_id);
    columns.op_action_right_sequence.push(action.right.sequence);
}

fn parse_delete_text_action_from_columns(
    columns: &mut Columns,
) -> Result<OperationAction, SerializationError> {
    let obj_ref = parse_obj_ref_from_columns(columns)?;

    let left_client_id = *columns.op_action_left_client_id.read()?;
    let left_sequence = *columns.op_action_left_sequence.read()?;
    let left = SequenceBlockId {
        client_id: left_client_id,
        sequence: left_sequence,
    };

    let right_client_id = *columns.op_action_right_client_id.read()?;
    let right_sequence = *columns.op_action_right_sequence.read()?;
    let right = SequenceBlockId {
        client_id: right_client_id,
        sequence: right_sequence,
    };

    Ok(OperationAction::DeleteText(crate::DeleteTextAction {
        object: obj_ref,
        left,
        right,
    }))
}

fn compare_operations(a: &&Operation, b: &&Operation) -> Ordering {
    if a.id.client_id == b.id.client_id {
        a.id.sequence.cmp(&b.id.sequence)
    } else {
        a.id.client_id.cmp(&b.id.client_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_two_way_sequential_compression_mixed() {
        let compressed = TwoWaySequenceCompressionStrategy::compress(&[1, 2, 5, 4, 3]);
        assert_eq!(
            compressed,
            [
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Increasing,
                    start: 1,
                    count: 2
                },
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Decreasing,
                    start: 5,
                    count: 3
                },
            ]
        );
    }

    #[test]
    fn test_two_way_sequential_compression_decrease_first() {
        let compressed = TwoWaySequenceCompressionStrategy::compress(&[5, 4, 3, 1, 2]);
        assert_eq!(
            compressed,
            [
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Decreasing,
                    start: 5,
                    count: 3
                },
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Increasing,
                    start: 1,
                    count: 2
                },
            ]
        );
    }

    #[test]
    fn test_two_way_sequential_compression_with_gaps() {
        let compressed = TwoWaySequenceCompressionStrategy::compress(&[1, 2, 4, 6, 5, 4, 3]);
        assert_eq!(
            compressed,
            [
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Increasing,
                    start: 1,
                    count: 2
                },
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Increasing,
                    start: 4,
                    count: 1
                },
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Decreasing,
                    start: 6,
                    count: 4
                },
            ]
        );
    }

    #[test]
    fn test_two_way_sequential_compression_ambiguous() {
        let compressed = TwoWaySequenceCompressionStrategy::compress(&[1, 2, 3, 2, 1]);
        assert_eq!(
            compressed,
            [
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Increasing,
                    start: 1,
                    count: 3
                },
                TwoWaySequenceRange {
                    direction: TwoWaySequenceRangeDirection::Decreasing,
                    start: 2,
                    count: 2
                },
            ]
        );
    }
}
