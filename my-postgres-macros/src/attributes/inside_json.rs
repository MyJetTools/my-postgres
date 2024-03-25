use types_reader::macros::*;

#[attribute_name("inside_json")]
#[derive(MacrosParameters)]
pub struct InsideJsonAttribute<'s> {
    #[default]
    pub name: &'s str,
}
