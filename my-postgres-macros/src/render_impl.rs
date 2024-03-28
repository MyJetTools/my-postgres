use quote::quote;
use types_reader::{GenericsArrayToken, TypeName};

pub fn implement_select_value_provider(
    type_name: &TypeName,
    content: impl Fn() -> proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let content = content();
    render_implement_trait(
        type_name,
        quote!(my_postgres::sql_select::SelectValueProvider),
        || {
            quote::quote! {
                fn fill_select_part(sql: &mut my_postgres::sql::SelectBuilder, field_name: &'static str, metadata: &Option<my_postgres::SqlValueMetadata>) {
                    #content
                }
            }
        },
    )
}

pub fn impl_from_db_row(
    type_name: &TypeName,
    fn_from_db_row: impl Fn() -> proc_macro2::TokenStream,
    fn_from_db_row_opt: impl Fn() -> proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let full_name_ident = type_name.to_token_stream();

    let name_no_generics_ident = type_name.get_name_ident();

    let trait_name = quote::quote!(my_postgres::sql_select::FromDbRow<'s, #full_name_ident>);

    let fn_from_db_row = fn_from_db_row();
    let fn_from_db_row_opt = fn_from_db_row_opt();

    render_implement_trait(type_name, trait_name, || {
        quote::quote! {
            fn from_db_row(row: &'s my_postgres::DbRow, name: &str, metadata: &Option<my_postgres::SqlValueMetadata>) -> #name_no_generics_ident {
                #fn_from_db_row
            }

            fn from_db_row_opt(row: &'s my_postgres::DbRow, name: &str, metadata: &Option<my_postgres::SqlValueMetadata>) -> Option<#name_no_generics_ident> {
                #fn_from_db_row_opt
            }
        }
    })
}

pub fn impl_sql_update_value_provider(
    type_name: &TypeName,
    fn_get_update_value: impl Fn() -> proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let fn_get_update_value = fn_get_update_value();
    render_implement_trait(
        type_name,
        quote::quote!(my_postgres::sql_update::SqlUpdateValueProvider),
        || {
            quote::quote! {
                fn get_update_value(
                    &self,
                    params: &mut my_postgres::sql::SqlValues,
                    metadata: &Option<my_postgres::SqlValueMetadata>,
                )->my_postgres::sql::SqlUpdateValue {
                    #fn_get_update_value
                }
            }
        },
    )
}

pub fn impl_sql_type_provider(
    type_name: &TypeName,
    fn_get_sql_type: impl Fn() -> proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let fn_get_sql_type = fn_get_sql_type();
    render_implement_trait(
        type_name,
        quote::quote!(my_postgres::table_schema::SqlTypeProvider),
        || {
            quote::quote! {
                fn get_sql_type(
                    meta_data: Option<my_postgres::SqlValueMetadata>,
                ) -> my_postgres::table_schema::TableColumnType {
                    #fn_get_sql_type
                }
            }
        },
    )
}

pub fn impl_sql_update_model(
    type_name: &TypeName,
    fn_get_fields_amount: proc_macro2::TokenStream,
    fn_get_column_name: proc_macro2::TokenStream,
    fn_get_field_value: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    render_implement_trait(
        type_name,
        quote::quote!(my_postgres::sql_update::SqlUpdateModel),
        || {
            quote::quote! {
                fn get_fields_amount() -> usize{
                    #fn_get_fields_amount
                }

                fn get_column_name(no: usize) -> my_postgres::ColumnName{
                    #fn_get_column_name
                }

                fn get_field_value(&self, no: usize) -> my_postgres::sql_update::SqlUpdateModelValue{
                    #fn_get_field_value
                }
            }
        },
    )
}

pub fn impl_sql_where_model(
    type_name: &TypeName,
    fn_fill_where_component: proc_macro2::TokenStream,
    fn_has_conditions: proc_macro2::TokenStream,
    fn_get_limit: proc_macro2::TokenStream,
    fn_get_offset: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    render_implement_trait(
        type_name,
        quote::quote!(my_postgres::sql_where::SqlWhereModel),
        || {
            quote::quote! {
                fn fill_where_component(&self, sql: &mut String, params: &mut my_postgres::sql::SqlValues){
                    #fn_fill_where_component
                }

                fn has_conditions(&self) -> bool{
                    #fn_has_conditions
                }

                fn get_limit(&self) -> Option<usize> {
                    #fn_get_limit
                }

                fn get_offset(&self) -> Option<usize> {
                    #fn_get_offset
                }

            }
        },
    )
}

fn render_implement_trait(
    type_name: &TypeName,
    trait_name: proc_macro2::TokenStream,
    content: impl Fn() -> proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let mut generics_after_impl_token = GenericsArrayToken::new();

    if let Some(life_time) = type_name.get_any_life_time() {
        generics_after_impl_token.add_life_time_if_not_exists(life_time);
    }

    let trait_name: TypeName = trait_name.try_into().unwrap();

    if let Some(life_time) = trait_name.get_any_life_time() {
        generics_after_impl_token.add_life_time_if_not_exists(life_time);
    }

    let content = content();

    let generic_after_impl = generics_after_impl_token.to_token_stream();

    let name_ident = type_name.to_token_stream();

    let trait_name = trait_name.to_token_stream();

    quote::quote! {
        impl #generic_after_impl #trait_name for #name_ident{
            #content
        }
    }
}
