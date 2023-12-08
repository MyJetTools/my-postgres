use types_reader::macros::*;

#[attribute_name("generate_select_model")]
#[derive(MacrosParameters)]
pub struct GenerateSelectModelAttribute {
    #[default]
    pub name: String,
}
