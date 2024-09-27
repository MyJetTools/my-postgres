use types_reader::macros::*;

#[attribute_name("wrap_column_name")]
#[derive(MacrosParameters)]
pub struct WrapColumnNameAttribute {
    #[default]
    pub name: String,
}
