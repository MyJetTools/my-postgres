use types_reader::macros::*;

#[attribute_name("enum_case")]
#[derive(MacrosParameters)]
pub struct EnumCaseAttribute<'s> {
    #[default]
    #[any_value_as_string]
    pub value: Option<&'s str>,
    #[any_value_as_string]
    pub id: Option<&'s str>,
}
