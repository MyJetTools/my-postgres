use quote::quote;
use types_reader::EnumCase;

use crate::consts::SqlTypeToRender;

use super::enum_case_ext::EnumCaseExt;

pub fn generate_as_string_with_model(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let enum_name = &ast.ident;

    let enum_cases =  EnumCase::read(ast)?;

    let fn_to_str =  generate_fn_to_str(&enum_cases)?;

    let fn_from_str =  generate_fn_from_str(&enum_cases)?;


    let update_value_provider_fn_body = super::utils::render_update_value_provider_fn_body_as_json();

    let select_part = super::utils::render_select_part_as_json();

    let db_field_type = crate::render_impl:: get_column_type_as_parameter();


    let fn_body_from_db_row = crate::consts::render_fn_from_db_row_with_transformation();

    let fn_body_from_db_row_opt = crate::consts::render_fn_from_db_row_opt_with_transformation();


    let json_type = SqlTypeToRender::JsonB.to_token_stream();

    let result = quote! {

        impl #enum_name{

     
            pub fn to_str(&self)->String {
                match self{
                    #fn_to_str
                }
            }


            pub fn from_str(src: &str)->Self{
                let first_line_reader = src.into();
                let (name, model) = my_postgres::utils::get_case_and_model(&first_line_reader);
                let name = name.as_str().unwrap();
                match name.as_str() {
                    #fn_from_str
                  _ => panic!("Invalid value {}", name.as_str())
                }
            }

            pub fn fill_select_part(sql: &mut my_postgres::sql::SelectBuilder, column_name: #db_field_type,  metadata: &Option<my_postgres::SqlValueMetadata>) {
                #select_part
            }

            fn get_sql_type() -> my_postgres::table_schema::TableColumnType {
                #json_type
            }
        }

            impl<'s> my_postgres::sql_select::FromDbRow<'s, #enum_name> for #enum_name{
                fn from_db_row(row: &'s my_postgres::DbRow, column_name: #db_field_type, metadata: &Option<my_postgres::SqlValueMetadata>) -> Self{
                    #fn_body_from_db_row
                }

                fn from_db_row_opt(row: &'s my_postgres::DbRow, column_name: #db_field_type, metadata: &Option<my_postgres::SqlValueMetadata>) -> Option<Self>{
                    #fn_body_from_db_row_opt
                }
            }

            impl my_postgres::sql_update::SqlUpdateValueProvider for #enum_name{
                fn get_update_value(
                    &self,
                    params: &mut my_postgres::sql::SqlValues,
                    metadata: &Option<my_postgres::SqlValueMetadata>,
                )->my_postgres::sql::SqlUpdateValue {
                    #update_value_provider_fn_body
                }

            }
    
    }
    .into();

    Ok(result)
}

fn generate_fn_from_str(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = proc_macro2::TokenStream::new();
    for case in enum_cases {
        let case_ident = &case.get_name_ident();

        let case_value = case.get_value()?.get_value_as_str();
        let case_value = case_value.as_str();

        if case.model.is_none() {
            return Err(syn::Error::new_spanned(
                case_value,
                "Model is not defined for this enum case",
            ));
        }

        let model = case.model.as_ref().unwrap().get_name_ident();

        result.extend(quote! {
            #case_value => Self::#case_ident(#model::from_str(model.as_raw_str().unwrap())),
        });
    }
    Ok(result)
}

fn generate_fn_to_str(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = proc_macro2::TokenStream::new();
    for case in enum_cases {
        let case_ident = &case.get_name_ident();

        let case_value = case.get_value()?.get_value_as_str();
        let case_value = case_value.as_str();

        result.extend(quote! {
            Self::#case_ident(model) => my_postgres::utils::compile_enum_with_model(#case_value, model.to_string().as_str()),
        });
    }
    Ok(result)
}
