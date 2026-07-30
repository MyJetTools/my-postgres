/// Tells my-postgres to handle a `Vec<u8>` as a postgres `bytea` column.
///
/// Derive macros substitute this type for `Vec<u8>` fields, so a DTO keeps working with
/// a plain `Vec<u8>` field. `#[sql_type("json")]` or `#[sql_type("jsonb")]` on a `Vec<u8>`
/// field opts back into serializing it as a json array.
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct SqlBinary(Vec<u8>);

impl SqlBinary {
    pub fn new(value: Vec<u8>) -> Self {
        Self(value)
    }

    /// Borrows a `Vec<u8>` as a `SqlBinary` - generated code uses it to get
    /// a `&dyn SqlUpdateValueProvider` out of a `Vec<u8>` field.
    pub fn from_ref(src: &Vec<u8>) -> &Self {
        // SqlBinary is a repr(transparent) wrapper around Vec<u8>,
        // which makes both types having the same memory layout
        unsafe { &*(src as *const Vec<u8> as *const Self) }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl Into<SqlBinary> for Vec<u8> {
    fn into(self) -> SqlBinary {
        SqlBinary::new(self)
    }
}
