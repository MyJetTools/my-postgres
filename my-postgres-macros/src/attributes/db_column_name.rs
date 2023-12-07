use types_reader::macros::*;

#[attribute_name("db_column_name")]
#[derive(MacrosParameters)]
pub struct DbColumnNameAttribute<'s> {
    #[default]
    pub name: &'s str,
}

impl DbColumnNameAttribute<'_> {
    pub fn generate_attribute(&self) -> proc_macro2::TokenStream {
        let db_column_name = self.name;
        quote::quote! {
            #[db_column_name(#db_column_name)]
        }
    }
}
