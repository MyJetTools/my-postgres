use proc_macro::TokenStream;
use quote::quote;
use types_reader::StructureSchema;

pub fn generate(ast: &syn::DeriveInput) -> Result<TokenStream, syn::Error> {
    let structure_schema = StructureSchema::new(ast)?;

    let fn_select_fields = super::fn_fill_select_fields::fn_fill_select_fields(&structure_schema)?;

    let orders_by_fields = match super::fn_fill_order_by::fn_get_order_by_fields(&structure_schema)
    {
        Ok(result) => result,
        Err(err) => err.to_compile_error(),
    };

    let group_by_fields = match super::fn_fill_group_by::get_group_by_fields(&structure_schema) {
        Ok(result) => result,
        Err(err) => err.to_compile_error(),
    };

    let from_fields = match super::fn_from::fn_from(&structure_schema) {
        Ok(result) => result,
        Err(err) => vec![err.to_compile_error()],
    };

    let struct_name = structure_schema.name.get_name_ident();

    let result = quote! {
        impl my_postgres::sql_select::SelectEntity for #struct_name{

            fn fill_select_fields(sql: &mut my_postgres::sql::SelectBuilder) {
                use my_postgres::sql_select::SelectValueProvider;
                #(#fn_select_fields)*
            }

            fn get_order_by_fields() -> Option<&'static str>{
                #orders_by_fields
            }

            fn get_group_by_fields() -> Option<&'static str>{
               #group_by_fields
            }

            fn from(db_row: &my_postgres::DbRow) -> Self {
                use my_postgres::sql_select::FromDbRow;
                Self{
                 #(#from_fields)*
                }
            }
        }

    }
    .into();

    Ok(result)
}
