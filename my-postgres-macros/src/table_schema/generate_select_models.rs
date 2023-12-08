use std::str::FromStr;

use proc_macro2::TokenStream;

use crate::postgres_struct_schema::PostgresStructSchema;

pub fn generate_select_models<'s>(
    struct_schema: &'s impl PostgresStructSchema<'s>,
) -> Result<TokenStream, syn::Error> {
    let select_models = struct_schema.get_select_properties_to_generate()?;

    let mut result = Vec::new();

    for (struct_name, struct_props) in select_models {
        let struct_name = TokenStream::from_str(struct_name.as_str()).unwrap();

        let mut result_fields = Vec::new();

        for struct_property in struct_props {
            let field_name = proc_macro2::TokenStream::from_str(&struct_property.name).unwrap();
            let ty = &struct_property.ty.get_token_stream();

            //struct_property.fill_attributes(&mut result_fields, None)?;

            result_fields.push(quote::quote! {
                pub #field_name: #ty,
            });
        }

        result.push(quote::quote! {
            #[derive(my_postgres::macros::SelectDbEntity, Debug)]
            pub struct #struct_name{
                #(#result_fields)*
            }
        });
    }

    let result = quote::quote! {
        #(#result)*
    };

    Ok(result)
}
