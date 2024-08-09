use quote::quote;
use types_reader::EnumCase;

use super::enum_case_ext::EnumCaseExt;

pub fn generate_as_string(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let enum_name = &ast.ident;

    let enum_cases = EnumCase::read(ast)?;

    let fn_to_str = generate_fn_to_str(&enum_cases)?;

    let fn_from_str = generate_fn_from_str(&enum_cases)?;

    let default_value_reading = super::utils::get_default_value( enum_cases.as_slice())?;

    let impl_where_value_provider = crate::where_value_provider::render_where_value_provider(enum_name, ||{
        let operator_check = crate::where_value_provider::render_standard_operator_check("=");
        quote::quote!{

            #operator_check

            let index = params.push(self.to_str().into());
            sql.push('$');
            sql.push_str(index.to_string().as_str());
            true
        }
    });

    let result = quote! {

        impl #enum_name{

            #default_value_reading
            
            pub fn to_str(&self)->&'static str {
                match self{
                    #fn_to_str
                }
            }

            pub fn from_str(src: &str)->Self{
                match src{
                    #fn_from_str
                  _ => panic!("Invalid value {}", src)
                }
            }

            pub fn fill_select_part(sql: &mut my_postgres::sql::SelectBuilder, field_name: &'static str,db_column_name: &'static str, metadata: &Option<my_postgres::SqlValueMetadata>) {
                sql.push(my_postgres::sql::SelectFieldValue::Field(field_name));
            }
        }

        impl my_postgres::sql_update::SqlUpdateValueProvider for #enum_name{
            fn get_update_value(
                &self,
                params: &mut my_postgres::sql::SqlValues,
                metadata: &Option<my_postgres::SqlValueMetadata>,
            )->my_postgres::sql::SqlUpdateValue {
                let index = params.push_static_str(self.to_str());
                my_postgres::sql::SqlUpdateValue::Index(index)
            }

        }

        #impl_where_value_provider

        impl<'s> my_postgres::sql_select::FromDbRow<'s, #enum_name> for #enum_name{
            fn from_db_row(row: &'s my_postgres::DbRow, name: &str, metadata: &Option<my_postgres::SqlValueMetadata>) -> Self{
                let result: String = row.get(name);
                Self::from_str(result.as_str())
            }

            fn from_db_row_opt(row: &'s my_postgres::DbRow, name: &str, metadata: &Option<my_postgres::SqlValueMetadata>) -> Option<Self>{
                let result: Option<String> = row.get(name);
                let result = result?;
                Some(Self::from_str(result.as_str()))
            }
        }

        impl my_postgres::table_schema::SqlTypeProvider for #enum_name {
            fn get_sql_type(
                _metadata: Option<my_postgres::SqlValueMetadata>,
            ) -> my_postgres::table_schema::TableColumnType {
                my_postgres::table_schema::TableColumnType::Text
            }
        }

    }
    .into();

    Ok(result)
}

fn generate_fn_from_str(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = proc_macro2::TokenStream::new();
    for case in enum_cases {

        let case_value = case.get_value()?;
        let case_value = case_value.get_value_as_str();
        let case_value = case_value.as_str();

        let case_ident = case.get_name_ident();

        result.extend(quote! {
            #case_value => Self::#case_ident,
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
            Self::#case_ident => #case_value,
        });
    }
    Ok(result)
}
