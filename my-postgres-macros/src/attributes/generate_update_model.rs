use types_reader::macros::*;

#[derive(MacrosEnum)]
pub enum GenerateType {
    #[value("where")]
    Where,
    #[value("update")]
    Update,
}

#[attribute_name("generate_update_model")]
#[derive(MacrosParameters)]
pub struct GenerateUpdateModelAttribute {
    #[default]
    pub name: String,
    pub param_type: GenerateType,
}
