use quote::quote;
use types_reader::{TypeName, StructProperty};

use crate::postgres_struct_ext::PostgresStructPropertyExt;

pub fn generate(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let src_fields = StructProperty::read(ast)?;

    let type_name = TypeName::new(ast);

    let struct_name = type_name.get_type_name();


    let where_fields = generate_json_where_fields(&src_fields)?;



    let result = quote! {

        impl #struct_name{
            pub fn from_str(src:&str)->Self{
                serde_json::from_str(src).unwrap()
            }

            pub fn to_string(&self)->String{
                serde_json::to_string(self).unwrap()
            }
        }

        impl my_postgres::sql_select::SelectValueProvider for #struct_name {
            fn fill_select_part(sql: &mut my_postgres::sql::SelectBuilder, field_name: &'static str, metadata: &Option<my_postgres::SqlValueMetadata>) {
                sql.push(my_postgres::sql::SelectFieldValue::Json(field_name));
            }
        }

        impl<'s> my_postgres::sql_select::FromDbRow<'s, #struct_name> for #struct_name {
            fn from_db_row(row: &'s my_postgres::DbRow, name: &str, metadata: &Option<my_postgres::SqlValueMetadata>) -> #struct_name {
                let str_value: String = row.get(name);
                Self::from_str(str_value.as_str())                
            }

            fn from_db_row_opt(row: &'s my_postgres::DbRow, name: &str, metadata: &Option<my_postgres::SqlValueMetadata>) -> Option<#struct_name> {
                let str_value: Option<String> = row.get(name);
                let str_value = str_value.as_ref()?;
        
                let result = Self::from_str(str_value);
                Some(result)            
            }
        }

        impl my_postgres::sql_update::SqlUpdateValueProvider for #struct_name {
            fn get_update_value(
                &self,
                params: &mut my_postgres::sql::SqlValues,
                metadata: &Option<my_postgres::SqlValueMetadata>,
            )->my_postgres::sql::SqlUpdateValue {
                let index = params.push(self.to_string().into());
                my_postgres::sql::SqlUpdateValue::Json(index)
            }
        }

        impl my_postgres::table_schema::SqlTypeProvider for #struct_name {
            fn get_sql_type(
                meta_data: Option<my_postgres::SqlValueMetadata>,
            ) -> my_postgres::table_schema::TableColumnType {

                if let Some(meta_data) = &meta_data{
                    if let Some(sql_type) = meta_data.sql_type{
                        if sql_type == "jsonb"{
                            return my_postgres::table_schema::TableColumnType::Jsonb
                        }
                    }
                }

                my_postgres::table_schema::TableColumnType::Json
            }
        }

        impl my_postgres::SqlWhereValueProvider for #struct_name{
            fn get_where_value(
                &self,
                sql_values: &mut my_postgres::sql::SqlValues,
                metadata: &Option<my_postgres::SqlValueMetadata>,
            ) -> my_postgres::sql::SqlWhereValue {
                my_postgres::sql::SqlWhereValue::VecOfJsonProperties(Box::new(vec![
                    #where_fields
                ]))
            }
        
            fn get_default_operator(&self) -> &'static str {
                "="
            }
        
            fn is_none(&self) -> bool {
                false
            }
        }


    }.into();

    Ok(result)
}



fn generate_json_where_fields(src_fields: &Vec<StructProperty>)->Result<proc_macro2::TokenStream, syn::Error>{

    let mut fields = Vec::new();

    for src_field in src_fields{


        let field_name = src_field.name.as_str();
        let name_ident = src_field.get_field_name_ident();
        let db_column_name = src_field.get_db_column_name_as_string()?;

        fields.push(quote::quote!{
            my_postgres::sql::JsonPropertyValueProvider{
                db_column_name: #db_column_name,
                json_property_name: #field_name,
                value: self.#name_ident.get_where_value(sql_values, metadata),
            },
        });

    }

    Ok(quote::quote!(#(#fields)*))
    
}