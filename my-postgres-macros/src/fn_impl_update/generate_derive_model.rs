use types_reader::TypeName;

use crate::{postgres_struct_ext::PostgresStructPropertyExt, where_fields::WhereFields};

use super::update_fields::UpdateFields;

pub fn generate_derive_model(
    type_name: &TypeName,
    update_fields: UpdateFields,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let fn_get_column_name = get_columns(&update_fields)?;

    let get_field_value_case = super::fn_get_field_value::fn_get_field_value(&update_fields)?;

    let where_fields = update_fields
        .get_where_fields()
        .iter()
        .map(|x| *x)
        .collect();

    let where_fields = WhereFields {
        where_fields,
        limit: None,
        offset: None,
    };

    let where_impl = where_fields.generate_implementation(type_name)?;

    let sql_update_model_impl = crate::render_impl::impl_sql_update_model(
        type_name,
        {
            let fields_amount = update_fields.get_update_fields().len();
            quote::quote!(#fields_amount)
        },
        fn_get_column_name,
        quote::quote! {
                match no{
                    #(#get_field_value_case)*
                    _=>panic!("no such field with number {}", no)
                }

        },
    );

    Ok(quote::quote! {
        #sql_update_model_impl
        #where_impl
    }
    .into())
}

fn get_columns(fields: &UpdateFields) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut line = Vec::with_capacity(fields.get_fields_amount());
    let mut no: usize = 0;
    for field in fields.get_update_fields() {
        let db_column_name = field.get_db_column_name_as_string()?;

        line.push(quote::quote!(#no=>#db_column_name.into(),));
        no += 1;
    }

    let result = quote::quote! {
        match no{
          #(#line)*
          _=>panic!("no such field with number {}", no)
        }
    };

    Ok(result)
}
