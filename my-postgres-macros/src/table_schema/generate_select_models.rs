use std::{collections::HashMap, str::FromStr};

use proc_macro2::TokenStream;

use crate::{
    postgres_struct_ext::PostgresStructPropertyExt, postgres_struct_schema::PostgresStructSchema,
};

pub fn generate_select_models<'s>(
    struct_schema: &'s impl PostgresStructSchema<'s>,
) -> Result<TokenStream, syn::Error> {
    let mut found_fields = HashMap::new();

    for field in struct_schema.get_fields() {
        let where_models = field.get_generate_additional_select_models()?;

        if let Some(where_models) = where_models {
            for where_model in where_models {
                if !found_fields.contains_key(where_model.struct_name.as_str()) {
                    found_fields.insert(where_model.struct_name.to_string(), Vec::new());
                }

                found_fields
                    .get_mut(where_model.struct_name.as_str())
                    .unwrap()
                    .push((where_model, field));
            }
        }
    }

    let mut result = Vec::new();

    for (struct_name, models) in found_fields {
        let struct_name = TokenStream::from_str(struct_name.as_str()).unwrap();

        let mut fields = Vec::new();

        for (model, field) in models {
            let field_name = TokenStream::from_str(model.field_name.as_str()).unwrap();
            let ty = &model.field_ty;

            field.fill_attributes(&mut fields, None)?;

            fields.push(quote::quote! {
                pub #field_name: #ty,
            });
        }

        result.push(quote::quote! {
            #[derive(my_postgres::macros::SelectDbEntity, Debug)]
            pub struct #struct_name{
                #(#fields)*
            }
        });
    }

    let result = quote::quote! {
        #(#result)*
    };

    Ok(result)
}
