
use proc_macro2::TokenStream;

use types_reader::{ macros::MacrosEnum, StructureSchema};

use crate::{postgres_struct_ext::PostgresStructPropertyExt,  postgres_struct_schema::PostgresStructSchema};
#[derive(MacrosEnum)]
pub enum GenerateType{
    #[value("where")]
    Where,
    #[value("update")]
    Update,
}




pub fn generate(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let struct_schema = StructureSchema::new(ast)?;

    let db_columns = impl_db_columns(&struct_schema)?;

    let select_models = super::generate_select_models(&struct_schema)?;

    let update_models = super::generate_update_models(&struct_schema)?;
    let where_models = super::generate_where_models(&struct_schema)?;

    let result =quote::quote!{
        #db_columns

        #select_models
    
        #update_models

        #where_models
    }.into();

    Ok(result)
}

fn impl_db_columns<'s>(
    struct_schema: &'s impl PostgresStructSchema<'s>,
) -> Result<proc_macro2::TokenStream, syn::Error> {

    let mut columns = Vec::new();


    for prop in struct_schema.get_fields(){
        let db_column_name = prop.get_db_column_name()?;
        let db_column_name = db_column_name.as_str();
        let sql_type = prop.get_sql_type_as_token_stream()?;
        let is_option: bool = prop.ty.is_option();

        let default_value = if let Some(default_value) = prop.get_default_value()? {
            match default_value{
                crate::postgres_struct_ext::DefaultValue::Inherit => {
                    let type_name = prop.ty.get_token_stream();
                    quote::quote!(Some(#type_name::get_default_value().into()))
                },
                crate::postgres_struct_ext::DefaultValue::Value(default_value) => {
                    quote::quote!(Some(#default_value.into()))
                },
            }
            
        } else {
            quote::quote!(None)
        };
     

        columns.push(quote::quote! {
            TableColumn{
                name: #db_column_name.into(),
                sql_type: #sql_type,
                is_nullable: #is_option,
                default: #default_value
            }
        });
    }



    let idx_fields = struct_schema.get_db_index_fields()?;



    let primary_keys = if idx_fields.primary_keys.is_empty() {
        quote::quote!(None)
    } else {
        let mut result = Vec::new();
        for (_, value) in idx_fields.primary_keys {
            result.push(value);
        }
        quote::quote!(Some(vec![#(#result.into()),*]))
    };




    let indexes = if idx_fields.indexes.is_empty() {
        quote::quote!(None)
    } else {
        let mut quotes: Vec<TokenStream> = Vec::new();

        for (index_name, index_data) in idx_fields.indexes {
            let mut fields = Vec::new();

            let mut is_unique = false;

            for index_data in index_data.values() {
                is_unique = index_data.attr.is_unique;
                let name = &index_data.prop.get_db_column_name()?;
                let name = name.as_str();

                let order = index_data.attr.order.to_index_order_token_stream();
                fields.push(quote::quote!(IndexField { name: #name.into(), order: #order }));
            }


            quotes.push(quote::quote!(result.insert(#index_name.to_string(), IndexSchema::new(#is_unique, vec![#(#fields,)*]));));
        }

        quote::quote! {
            let mut result = std::collections::HashMap::new();
            #(#quotes;)*

            Some(result)
        }
    };

    let struct_name = struct_schema.get_name().get_name_ident();

    let result = quote::quote! {

        impl my_postgres::table_schema::TableSchemaProvider for #struct_name{
  

        fn get_primary_key_columns() -> Option<Vec<my_postgres::ColumnName>>{
          #primary_keys
        }
            fn get_columns() -> Vec<my_postgres::table_schema::TableColumn>{
                use my_postgres::table_schema::*;
                vec![#(#columns),*]
            }
            fn get_indexes() -> Option<std::collections::HashMap<String, my_postgres::table_schema::IndexSchema>>{
                use my_postgres::table_schema::*;
                #indexes
            }
        }
    }
    .into();

    Ok(result)
}

