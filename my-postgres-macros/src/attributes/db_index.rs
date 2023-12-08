use types_reader::macros::*;

#[derive(MacrosEnum)]
pub enum IndexOrder {
    #[value("ASC")]
    Asc,
    #[value("DESC")]
    Desc,
}

impl IndexOrder {
    pub fn to_index_order_token_stream(&self) -> proc_macro2::TokenStream {
        match self {
            IndexOrder::Asc => quote::quote!(IndexOrder::Asc),
            IndexOrder::Desc => quote::quote!(IndexOrder::Desc),
        }
    }
}

#[attribute_name("db_index")]
#[derive(MacrosParameters)]
pub struct DbIndexAttribute<'s> {
    pub index_name: &'s str,
    pub id: u8,
    pub is_unique: bool,
    pub order: IndexOrder,
}
