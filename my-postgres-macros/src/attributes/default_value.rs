use types_reader::macros::*;

#[attribute_name("default_value")]
#[derive(MacrosParameters)]
pub struct DefaultValueAttribute<'s> {
    #[default]
    pub value: Option<&'s str>,
}
