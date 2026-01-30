use std::fmt;

use num_derive::{FromPrimitive, ToPrimitive};

pub const ARRAY_DELIMIT: &[u8; 1] = b"=";
pub const ESCAPE: &[u8; 1] = b"\\";
pub const LIST_BEGIN: &[u8; 1] = b"[";
pub const LIST_END: &[u8; 1] = b"]";
pub const MAP_BEGIN: &[u8; 1] = b"{";
pub const MAP_END: &[u8; 1] = b"}";
pub const STRING_DELIMIT: &[u8; 1] = b"\"";
pub const TRUE: &[u8; 1] = &[1];
pub const FALSE: &[u8; 1] = &[0];

#[derive(FromPrimitive, ToPrimitive)]
pub enum Type {
    None = 1,
    Bool,
    F32,
    F64,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Bool => "boolean",
            Self::F32 => "32-bit float",
            Self::F64 => "64-bit float",
            Self::I8 => "8-bit int",
            Self::I16 => "16-bit int",
            Self::I32 => "32-bit int",
            Self::I64 => "64-bit int",
            Self::U8 => "8-bit unsigned int",
            Self::U16 => "16-bit unsigned int",
            Self::U32 => "32-bit unsigned int",
            Self::U64 => "64-bit unsigned int",
        })
    }
}
