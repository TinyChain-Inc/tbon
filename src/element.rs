use std::convert::TryInto;

use destream::de;

use super::constants::Type;

pub trait Element: Sized {
    const SIZE: usize;

    fn dtype() -> Type;

    fn from_bytes(bytes: &[u8]) -> Self;

    // TODO: use const generic Self::SIZE to return an array
    // fn to_bytes(&self) -> [u8; Self::SIZE];

    #[inline]
    fn parse<E: de::Error>(bytes: &[u8]) -> Result<Self, E> {
        if bytes.len() == Self::SIZE {
            Ok(Self::from_bytes(bytes))
        } else {
            Err(de::Error::invalid_length(bytes.len(), Self::SIZE))
        }
    }
}

pub trait IntoBytes<const SIZE: usize>: Sized {
    fn into_bytes(self) -> [u8; SIZE];
}

impl Element for bool {
    const SIZE: usize = 1;

    fn dtype() -> Type {
        Type::Bool
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        bytes[0] == 1
    }
}

impl IntoBytes<1> for bool {
    fn into_bytes(self) -> [u8; 1] {
        if self {
            [1]
        } else {
            [0]
        }
    }
}

impl Element for u8 {
    const SIZE: usize = 1;

    fn dtype() -> Type {
        Type::U8
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        bytes[0]
    }
}

impl IntoBytes<1> for u8 {
    fn into_bytes(self) -> [u8; 1] {
        [self]
    }
}

impl Element for u16 {
    const SIZE: usize = 2;

    fn dtype() -> Type {
        Type::U16
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<2> for u16 {
    fn into_bytes(self) -> [u8; 2] {
        self.to_be_bytes()
    }
}

impl Element for u32 {
    const SIZE: usize = 4;

    fn dtype() -> Type {
        Type::U32
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<4> for u32 {
    fn into_bytes(self) -> [u8; 4] {
        self.to_be_bytes()
    }
}

impl Element for u64 {
    const SIZE: usize = 8;

    fn dtype() -> Type {
        Type::U64
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<8> for u64 {
    fn into_bytes(self) -> [u8; 8] {
        self.to_be_bytes()
    }
}

impl Element for i8 {
    const SIZE: usize = 1;

    fn dtype() -> Type {
        Type::I8
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<1> for i8 {
    fn into_bytes(self) -> [u8; 1] {
        self.to_be_bytes()
    }
}

impl Element for i16 {
    const SIZE: usize = 2;

    fn dtype() -> Type {
        Type::I16
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<2> for i16 {
    fn into_bytes(self) -> [u8; 2] {
        self.to_be_bytes()
    }
}

impl Element for i32 {
    const SIZE: usize = 4;

    fn dtype() -> Type {
        Type::I32
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<4> for i32 {
    fn into_bytes(self) -> [u8; 4] {
        self.to_be_bytes()
    }
}

impl Element for i64 {
    const SIZE: usize = 8;

    fn dtype() -> Type {
        Type::I64
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<8> for i64 {
    fn into_bytes(self) -> [u8; 8] {
        self.to_be_bytes()
    }
}

impl Element for f32 {
    const SIZE: usize = 4;

    fn dtype() -> Type {
        Type::F32
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<4> for f32 {
    fn into_bytes(self) -> [u8; 4] {
        self.to_be_bytes()
    }
}

impl Element for f64 {
    const SIZE: usize = 8;

    fn dtype() -> Type {
        Type::F64
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl IntoBytes<8> for f64 {
    fn into_bytes(self) -> [u8; 8] {
        self.to_be_bytes()
    }
}
