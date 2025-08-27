use proc_macro2::TokenStream;
use quote::quote;
use types_reader::EnumCase;

use super::enum_case_ext::EnumCaseExt;

pub fn generate_with_model(ast: &syn::DeriveInput) -> Result<TokenStream, syn::Error> {
    let enum_name = &ast.ident;

    let enum_cases = EnumCase::read(ast)?;

    let fn_to_str = fn_to_str(enum_cases.as_slice())?;

    let from_db_value =  fn_from_db_value(enum_cases.as_slice())?;

    let select_part = super::utils::render_select_part_as_json();

    let update_value_provider_fn_body = super::utils::render_update_value_provider_fn_body_as_json();
    let db_field_type = crate::render_impl::get_column_type_as_parameter();

    let fn_body_from_db_row = crate::consts::render_fn_from_db_row_with_transformation();

    let fn_body_from_db_row_opt = crate::consts::render_fn_from_db_row_opt_with_transformation();
    
    let result = quote! {

        impl #enum_name{

            pub fn to_str(&self)->String {
                match self{
                #(#fn_to_str),*
                }
            }

  

            pub fn from_str(src: &str)->Self{
                let first_line_reader = src.into();
                let (case, model) = my_postgres::utils::get_case_and_model(&first_line_reader);
                let name = case.as_str().unwrap();
                match name.as_str(){
                  #(#from_db_value)*
                  _ => panic!("Invalid value {}", name.as_str())
                }
            }

            pub fn fill_select_part(sql: &mut my_postgres::sql::SelectBuilder,  column_name: #db_field_type ,  metadata: &Option<my_postgres::SqlValueMetadata>) {
               #select_part
            }

            fn get_sql_type() -> my_postgres::table_schema::TableColumnType {
                my_postgres::table_schema::TableColumnType::Jsonb
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

       

        impl<'s> my_postgres::sql_select::FromDbRow<'s, #enum_name> for #enum_name{
            fn from_db_row(row: &'s my_postgres::DbRow, column_name: #db_field_type, metadata: &Option<my_postgres::SqlValueMetadata>) -> Self{
                #fn_body_from_db_row
            }

            fn from_db_row_opt(row: &'s my_postgres::DbRow, column_name: #db_field_type, metadata: &Option<my_postgres::SqlValueMetadata>) -> Option<Self>{
                #fn_body_from_db_row_opt
            }
        }


    }
    .into();

    Ok(result)
}

pub fn fn_to_str(enum_cases: &[EnumCase]) -> Result<Vec<TokenStream>, syn::Error> {
    let mut result = Vec::with_capacity(enum_cases.len());

    let mut no = 0;
    for enum_case in enum_cases {
        let enum_case_name = enum_case.get_name_ident();

        no = match enum_case.get_value()?.as_number()?{
            Some(value) => value,
            None => no+1,
        };
        
        let no = no.to_string();

        result.push(quote!(Self::#enum_case_name(model) => my_postgres::utils::compile_enum_with_model(#no, model.to_string().as_str())).into());
    }

    Ok(result)
}


fn fn_from_db_value(enum_cases: &[EnumCase]) -> Result<Vec<TokenStream>, syn::Error> {
    let mut result = Vec::with_capacity(enum_cases.len());
    let mut no= 0;

    for enum_case in enum_cases {
        let name_ident = enum_case.get_name_ident();

        if enum_case.model.is_none() {
            return Err(syn::Error::new_spanned(
                enum_case.get_name_ident(),
                "Model is not defined for this enum case",
            ));
        }

        let model = enum_case.model.as_ref().unwrap().get_name_ident();

        no = match enum_case.get_value()?.as_number()?{
            Some(value) => value,
            None => no+1,
        };
        
        let no = no.to_string();
        result.push(quote! (#no => Self::#name_ident(#model::from_str(model.as_raw_str().unwrap())),));
    }

    Ok(result)
}


/*
          pub fn to_numbered(&self)->(#type_name, String) {
                match self{
                #(#fn_to_numbered),*
                }
            }
*/