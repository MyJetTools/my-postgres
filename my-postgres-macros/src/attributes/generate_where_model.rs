use types_reader::macros::*;

use super::WhereOperator;

#[attribute_name("generate_where_model")]
#[derive(MacrosParameters)]
pub struct GenerateWhereModelAttribute {
    #[default]
    pub name: String,
    pub operator: Option<WhereOperator>,
    pub operator_from: Option<WhereOperator>,
    pub operator_to: Option<WhereOperator>,
    #[has_attribute]
    pub as_str: bool,
    #[has_attribute]
    pub as_vec: bool,
    #[has_attribute]
    pub as_option: bool,
    #[has_attribute]
    pub ignore_if_none: bool,
    pub limit: Option<String>,
}
