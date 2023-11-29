use quote::quote;
use types_reader::{TypeName, StructProperty};

use crate::postgres_struct_ext::PostgresStructPropertyExt;

pub fn generate(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let ident = &ast.ident;
    let src_fields = StructProperty::read(ast)?;

    let type_name = TypeName::new(ast);

    let struct_name = type_name.get_type_name();


    let where_fields = generate_json_where_fields(&src_fields)?;

    let impl_where_value_provider = crate::where_value_provider::render_where_value_provider(&ident, ||{
        quote::quote!{
            let mut json_column_name = "";
            if let Some(full_condition) = &full_where_condition {
                if full_condition.condition_no>0{
                    sql.push_str(" AND ");
                }

                json_column_name = full_condition.column_name;
            }
            #where_fields
        }
    });


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
        #impl_where_value_provider


    }.into();

    Ok(result)
}



fn generate_json_where_fields(src_fields: &Vec<StructProperty>)->Result<proc_macro2::TokenStream, syn::Error>{

    let mut lines = Vec::new();

    lines.push(quote::quote!(let mut condition_no = 0;));

    if lines.len()>0{
        lines.push(quote::quote!(sql.push('(');));
    }

    for src_field in src_fields{

        let prop_name_ident = src_field.get_field_name_ident();
        let db_column_name = src_field.get_db_column_name_as_string()?;
        let metadata = src_field.get_field_metadata()?;

        let where_condition = crate::where_fields::render_full_where_condition(db_column_name, Some("json_column_name"));

        if src_field.ty.is_option() {
            lines.push(quote::quote! {
                self.#prop_name_ident.fill_where_value(#where_condition, sql, params, &#metadata);
                condition_no+=1;
            });
        }else{
            lines.push(quote::quote! {
                self.#prop_name_ident.fill_where_value(#where_condition, sql, params, &#metadata);
                condition_no+=1;
            });
        }

    }


    if lines.len()>0{
        lines.push(quote::quote!(sql.push(')');));
    }

    Ok(quote::quote!(#(#lines)*))
    
}