use quote::quote;
use types_reader::{GenericsArrayToken, StructProperty, TypeName};

pub fn implement_select_value_provider(
    type_name: &TypeName,
    content: impl Fn() -> proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let content = content();
    let db_field_type = get_column_type_as_parameter();
    render_implement_trait(
        type_name,
        quote!(my_postgres::sql_select::SelectValueProvider),
        || {
            quote::quote! {
                fn fill_select_part(sql: &mut my_postgres::sql::SelectBuilder, column_name: #db_field_type  ,  metadata: &Option<my_postgres::SqlValueMetadata>) {
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

    let column_name_type = get_column_type_as_parameter();

    render_implement_trait(type_name, trait_name, || {
        quote::quote! {
            fn from_db_row(row: &'s my_postgres::DbRow, column_name: #column_name_type, metadata: &Option<my_postgres::SqlValueMetadata>) -> #name_no_generics_ident {
                #fn_from_db_row
            }

            fn from_db_row_opt(row: &'s my_postgres::DbRow, column_name: #column_name_type, metadata: &Option<my_postgres::SqlValueMetadata>) -> Option<#name_no_generics_ident> {
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

pub fn impl_sql_update_model<'s>(
    type_name: &TypeName,
    fn_get_fields_amount: proc_macro2::TokenStream,
    fn_get_column_name: proc_macro2::TokenStream,
    fn_get_field_value: proc_macro2::TokenStream,
    primary_key_fields: &'s [&'s StructProperty<'s>],
) -> proc_macro2::TokenStream {
    let mut primary_key_rendering = Vec::new();

    let mut i = 0;
    for prop in primary_key_fields {
        let name = prop.get_field_name_ident();

        if i > 0 {
            primary_key_rendering.push(quote::quote! {
                result.push('|');
            });
        }

        if prop.ty.is_string() {
            primary_key_rendering.push(quote::quote! {
                result.push_str(&self.#name);
            });
        } else if prop.ty.is_date_time() {
            primary_key_rendering.push(quote::quote! {
                result.push_str(&self.#name.to_rfc3339().as_str());
            });
        } else if prop.ty.is_simple_type() {
            primary_key_rendering.push(quote::quote! {
                result.push_str(&self.#name.to_string().as_str());
            });
        } else {
            primary_key_rendering.push(quote::quote! {
                result.push_str(&self.#name.to_str());
            });
        }
        i += 1;
    }

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

                fn get_primary_key_as_single_string(&self) -> String{
                    let mut result = String::new();
                    #( #primary_key_rendering )*
                    result
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

pub fn get_column_type_as_parameter() -> proc_macro2::TokenStream {
    quote! { my_postgres::DbColumnName  }
}
