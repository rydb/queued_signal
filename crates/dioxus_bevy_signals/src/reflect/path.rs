//! Field path navigation and leaf primitive helpers for reflected values.

use bevy_reflect::{Reflect, ReflectMut, ReflectRef};

/// One step in a path into a reflected value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ReflectPathSegment {
    /// A named struct field.
    Field(String),
}

impl ReflectPathSegment {
    /// The field name for this segment, when it targets a field.
    pub fn as_str(&self) -> &str {
        match self {
            ReflectPathSegment::Field(name) => name,
        }
    }
}

/// A sequence of segments locating a leaf inside a reflected value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReflectPath(Vec<ReflectPathSegment>);

impl ReflectPath {
    /// An empty path pointing at the root value.
    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// Append a named field segment to this path.
    pub fn field(&self, name: impl Into<String>) -> Self {
        let mut path = self.clone();
        path.0.push(ReflectPathSegment::Field(name.into()));
        path
    }

    /// The segments in this path.
    pub fn segments(&self) -> &[ReflectPathSegment] {
        &self.0
    }
}

/// Navigate to a reflected value along a path of named struct fields.
pub fn read_at_path<'a>(mut value: &'a dyn Reflect, path: &ReflectPath) -> Option<&'a dyn Reflect> {
    for segment in path.segments() {
        let ReflectRef::Struct(s) = value.reflect_ref() else {
            return None;
        };
        let field = s.field(segment.as_str())?;
        value = field.try_as_reflect()?;
    }
    Some(value)
}

/// Write a primitive leaf value along a path of named struct fields.
pub fn write_at_path(
    root: &mut dyn Reflect,
    path: &ReflectPath,
    replacement: &PrimitiveValue,
) -> bool {
    let segments = path.segments();
    let Some((last, parents)) = segments.split_last() else {
        return false;
    };

    let mut current: &mut dyn Reflect = root;
    for segment in parents {
        let ReflectMut::Struct(s) = current.reflect_mut() else {
            return false;
        };
        let Some(field) = s.field_mut(segment.as_str()) else {
            return false;
        };
        let Some(field) = field.try_as_reflect_mut() else {
            return false;
        };
        current = field;
    }

    let ReflectMut::Struct(s) = current.reflect_mut() else {
        return false;
    };
    let Some(leaf) = s.field_mut(last.as_str()) else {
        return false;
    };
    let Some(leaf) = leaf.try_as_reflect_mut() else {
        return false;
    };
    write_primitive(leaf, replacement)
}

/// The type of a leaf primitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PrimitiveKind {
    /// A boolean.
    Bool,
    /// A signed 8 bit integer.
    I8,
    /// A signed 16 bit integer.
    I16,
    /// A signed 32 bit integer.
    I32,
    /// A signed 64 bit integer.
    I64,
    /// An unsigned 8 bit integer.
    U8,
    /// An unsigned 16 bit integer.
    U16,
    /// An unsigned 32 bit integer.
    U32,
    /// An unsigned 64 bit integer.
    U64,
    /// A 32 bit float.
    F32,
    /// A 64 bit float.
    F64,
    /// A string.
    String,
}

/// An owned leaf primitive value.
#[derive(Clone, Debug, PartialEq)]
pub enum PrimitiveValue {
    /// A boolean.
    Bool(bool),
    /// A signed 8 bit integer.
    I8(i8),
    /// A signed 16 bit integer.
    I16(i16),
    /// A signed 32 bit integer.
    I32(i32),
    /// A signed 64 bit integer.
    I64(i64),
    /// An unsigned 8 bit integer.
    U8(u8),
    /// An unsigned 16 bit integer.
    U16(u16),
    /// An unsigned 32 bit integer.
    U32(u32),
    /// An unsigned 64 bit integer.
    U64(u64),
    /// A 32 bit float.
    F32(f32),
    /// A 64 bit float.
    F64(f64),
    /// A string.
    String(String),
}

impl PrimitiveValue {
    /// The kind of this primitive value.
    pub fn kind(&self) -> PrimitiveKind {
        match self {
            PrimitiveValue::Bool(_) => PrimitiveKind::Bool,
            PrimitiveValue::I8(_) => PrimitiveKind::I8,
            PrimitiveValue::I16(_) => PrimitiveKind::I16,
            PrimitiveValue::I32(_) => PrimitiveKind::I32,
            PrimitiveValue::I64(_) => PrimitiveKind::I64,
            PrimitiveValue::U8(_) => PrimitiveKind::U8,
            PrimitiveValue::U16(_) => PrimitiveKind::U16,
            PrimitiveValue::U32(_) => PrimitiveKind::U32,
            PrimitiveValue::U64(_) => PrimitiveKind::U64,
            PrimitiveValue::F32(_) => PrimitiveKind::F32,
            PrimitiveValue::F64(_) => PrimitiveKind::F64,
            PrimitiveValue::String(_) => PrimitiveKind::String,
        }
    }

    /// A display string for this value, suitable for a text input.
    pub fn to_string_repr(&self) -> String {
        match self {
            PrimitiveValue::Bool(v) => v.to_string(),
            PrimitiveValue::I8(v) => v.to_string(),
            PrimitiveValue::I16(v) => v.to_string(),
            PrimitiveValue::I32(v) => v.to_string(),
            PrimitiveValue::I64(v) => v.to_string(),
            PrimitiveValue::U8(v) => v.to_string(),
            PrimitiveValue::U16(v) => v.to_string(),
            PrimitiveValue::U32(v) => v.to_string(),
            PrimitiveValue::U64(v) => v.to_string(),
            PrimitiveValue::F32(v) => v.to_string(),
            PrimitiveValue::F64(v) => v.to_string(),
            PrimitiveValue::String(v) => v.clone(),
        }
    }

    /// Parse a string into a primitive of the given kind.
    pub fn parse(text: &str, kind: PrimitiveKind) -> Option<PrimitiveValue> {
        match kind {
            PrimitiveKind::Bool => text
                .parse::<bool>()
                .ok()
                .map(PrimitiveValue::Bool),
            PrimitiveKind::I8 => text.parse::<i8>().ok().map(PrimitiveValue::I8),
            PrimitiveKind::I16 => text.parse::<i16>().ok().map(PrimitiveValue::I16),
            PrimitiveKind::I32 => text.parse::<i32>().ok().map(PrimitiveValue::I32),
            PrimitiveKind::I64 => text.parse::<i64>().ok().map(PrimitiveValue::I64),
            PrimitiveKind::U8 => text.parse::<u8>().ok().map(PrimitiveValue::U8),
            PrimitiveKind::U16 => text.parse::<u16>().ok().map(PrimitiveValue::U16),
            PrimitiveKind::U32 => text.parse::<u32>().ok().map(PrimitiveValue::U32),
            PrimitiveKind::U64 => text.parse::<u64>().ok().map(PrimitiveValue::U64),
            PrimitiveKind::F32 => text.parse::<f32>().ok().map(PrimitiveValue::F32),
            PrimitiveKind::F64 => text.parse::<f64>().ok().map(PrimitiveValue::F64),
            PrimitiveKind::String => Some(PrimitiveValue::String(text.to_owned())),
        }
    }
}

/// Downcast a reflected value into an owned primitive, when it is one.
pub fn reflect_to_primitive(value: &dyn Reflect) -> Option<PrimitiveValue> {
    if let Some(v) = value.downcast_ref::<bool>() {
        return Some(PrimitiveValue::Bool(*v));
    }
    if let Some(v) = value.downcast_ref::<i8>() {
        return Some(PrimitiveValue::I8(*v));
    }
    if let Some(v) = value.downcast_ref::<i16>() {
        return Some(PrimitiveValue::I16(*v));
    }
    if let Some(v) = value.downcast_ref::<i32>() {
        return Some(PrimitiveValue::I32(*v));
    }
    if let Some(v) = value.downcast_ref::<i64>() {
        return Some(PrimitiveValue::I64(*v));
    }
    if let Some(v) = value.downcast_ref::<u8>() {
        return Some(PrimitiveValue::U8(*v));
    }
    if let Some(v) = value.downcast_ref::<u16>() {
        return Some(PrimitiveValue::U16(*v));
    }
    if let Some(v) = value.downcast_ref::<u32>() {
        return Some(PrimitiveValue::U32(*v));
    }
    if let Some(v) = value.downcast_ref::<u64>() {
        return Some(PrimitiveValue::U64(*v));
    }
    if let Some(v) = value.downcast_ref::<f32>() {
        return Some(PrimitiveValue::F32(*v));
    }
    if let Some(v) = value.downcast_ref::<f64>() {
        return Some(PrimitiveValue::F64(*v));
    }
    if let Some(v) = value.downcast_ref::<String>() {
        return Some(PrimitiveValue::String(v.clone()));
    }
    None
}

/// Box an owned primitive as a reflected value.
pub fn primitive_to_reflect(value: &PrimitiveValue) -> Box<dyn Reflect> {
    match value {
        PrimitiveValue::Bool(v) => Box::new(*v),
        PrimitiveValue::I8(v) => Box::new(*v),
        PrimitiveValue::I16(v) => Box::new(*v),
        PrimitiveValue::I32(v) => Box::new(*v),
        PrimitiveValue::I64(v) => Box::new(*v),
        PrimitiveValue::U8(v) => Box::new(*v),
        PrimitiveValue::U16(v) => Box::new(*v),
        PrimitiveValue::U32(v) => Box::new(*v),
        PrimitiveValue::U64(v) => Box::new(*v),
        PrimitiveValue::F32(v) => Box::new(*v),
        PrimitiveValue::F64(v) => Box::new(*v),
        PrimitiveValue::String(v) => Box::new(v.clone()),
    }
}

/// Assign a primitive into a reflected value, when the types match.
pub fn write_primitive(target: &mut dyn Reflect, value: &PrimitiveValue) -> bool {
    match value {
        PrimitiveValue::Bool(v) => target.downcast_mut::<bool>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::I8(v) => target.downcast_mut::<i8>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::I16(v) => target.downcast_mut::<i16>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::I32(v) => target.downcast_mut::<i32>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::I64(v) => target.downcast_mut::<i64>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::U8(v) => target.downcast_mut::<u8>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::U16(v) => target.downcast_mut::<u16>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::U32(v) => target.downcast_mut::<u32>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::U64(v) => target.downcast_mut::<u64>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::F32(v) => target.downcast_mut::<f32>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::F64(v) => target.downcast_mut::<f64>().is_some_and(|x| {
            *x = *v;
            true
        }),
        PrimitiveValue::String(v) => target.downcast_mut::<String>().is_some_and(|x| {
            *x = v.clone();
            true
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Reflect, Default, Debug, PartialEq)]
    struct Counter {
        value: i32,
    }

    #[derive(Reflect, Default, Debug, PartialEq)]
    struct Nested {
        inner: Counter,
        label: String,
    }

    #[test]
    fn read_primitive_field() {
        let counter = Counter { value: 7 };
        let path = ReflectPath::root().field("value");
        let leaf = read_at_path(counter.as_reflect(), &path).unwrap();
        assert_eq!(leaf.downcast_ref::<i32>(), Some(&7));
    }

    #[test]
    fn write_primitive_field() {
        let mut counter = Counter { value: 7 };
        let path = ReflectPath::root().field("value");
        assert!(write_at_path(
            counter.as_reflect_mut(),
            &path,
            &PrimitiveValue::I32(42)
        ));
        assert_eq!(counter.value, 42);
    }

    #[test]
    fn read_write_nested_field() {
        let mut nested = Nested {
            inner: Counter { value: 1 },
            label: "hi".into(),
        };
        let path = ReflectPath::root().field("inner").field("value");
        assert!(write_at_path(
            nested.as_reflect_mut(),
            &path,
            &PrimitiveValue::I32(9)
        ));
        assert_eq!(nested.inner.value, 9);

        let label_path = ReflectPath::root().field("label");
        assert!(write_at_path(
            nested.as_reflect_mut(),
            &label_path,
            &PrimitiveValue::String("yo".into())
        ));
        assert_eq!(nested.label, "yo");
    }

    #[test]
    fn write_mismatched_type_is_false() {
        let mut counter = Counter { value: 7 };
        let path = ReflectPath::root().field("value");
        assert!(!write_at_path(
            counter.as_reflect_mut(),
            &path,
            &PrimitiveValue::F32(1.0)
        ));
        assert_eq!(counter.value, 7);
    }

    #[test]
    fn primitive_roundtrip() {
        let value = PrimitiveValue::I32(5);
        let boxed = primitive_to_reflect(&value);
        assert_eq!(reflect_to_primitive(boxed.as_ref()), Some(value));
    }
}
