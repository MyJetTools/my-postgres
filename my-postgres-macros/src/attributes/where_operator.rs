use types_reader::macros::*;

#[derive(MacrosEnum)]
pub enum WhereOperator {
    #[value(">")]
    GreaterThan,
    #[value("<")]
    LessThan,
    #[value("=")]
    Equal,
    #[value(">=")]
    GreaterOrEqual,
    #[value("<=")]
    LessOrEqual,
    #[value("!=")]
    NotEqual,
    #[value("<>")]
    NotEqual2,
    #[value("like")]
    Like,
}

impl WhereOperator {
    pub fn get_metadata_operator(&self) -> &'static str {
        match self {
            Self::GreaterThan => ">",
            Self::LessThan => "<",
            Self::Equal => "=",
            Self::GreaterOrEqual => ">=",
            Self::LessOrEqual => "<=",
            Self::NotEqual => "!=",
            Self::NotEqual2 => "<>",
            Self::Like => " like ",
        }
    }
}

#[attribute_name("operator")]
#[derive(MacrosParameters)]
pub struct WhereOperatorAttribute {
    #[default]
    pub op: WhereOperator,
}
