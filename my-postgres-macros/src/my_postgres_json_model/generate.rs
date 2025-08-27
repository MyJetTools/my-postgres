use quote::quote;
use types_reader::TypeName;

pub fn generate(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let type_name: TypeName = ast.try_into()?;

    let type_impl = type_name.render_implement(|| {
        let type_name = type_name.get_name_ident().to_string();
        quote::quote! {
            pub fn from_str(src:&str)->Self{
                let result: Result<Self, _> = serde_json::from_str(src);

                if let Err(err) = &result{
                    panic!("Error parsing type '{}' from json '{}'. Err: {}", #type_name, src, err);
                }

                result.unwrap()
            }

            pub fn to_string(&self)->String{
                serde_json::to_string(self).unwrap()
            }
        }
    });

    let select_value_provider_impl =
        crate::render_impl::implement_select_value_provider(&type_name, || {
            quote::quote! {
                    sql.push(my_postgres::sql::SelectFieldValue::Json(column_name));
            }
        });

    let from_db_row_impl = crate::render_impl::impl_from_db_row(
        &type_name,
        || crate::consts::render_fn_from_db_row_with_transformation(),
        || crate::consts::render_fn_from_db_row_opt_with_transformation(),
    );

    let sql_update_value_provider_iml =
        crate::render_impl::impl_sql_update_value_provider(&type_name, || {
            quote::quote! {
                let index = params.push(self.to_string().into());
                my_postgres::sql::SqlUpdateValue::Json(index)
            }
        });

    let sql_type_provider_iml = crate::render_impl::impl_sql_type_provider(&type_name, || {
        quote::quote! {
            if let Some(meta_data) = &meta_data{
                return meta_data.sql_type;
            }

            my_postgres::table_schema::TableColumnType::Json
        }
    });

    let result = quote! {
        #type_impl

        #select_value_provider_impl

        #from_db_row_impl

        #sql_update_value_provider_iml

        #sql_type_provider_iml

    }
    .into();

    Ok(result)
}
