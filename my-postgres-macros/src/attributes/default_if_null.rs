use types_reader::macros::*;

//todo!("Not used")
#[attribute_name("default_if_null")]
#[derive(MacrosParameters)]
pub struct DefaultIfNullAttribute<'s> {
    #[default]
    pub value: &'s str,
}
