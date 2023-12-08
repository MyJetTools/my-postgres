use std::str::FromStr;

use proc_macro2::TokenStream;

use crate::{
    postgres_struct_ext::PostgresStructPropertyExt,
    postgres_struct_schema::{GenerateAdditionalWhereStruct, PostgresStructSchema},
};

pub fn generate_where_models<'s>(
    struct_schema: &'s impl PostgresStructSchema<'s>,
) -> Result<TokenStream, syn::Error> {
    let where_fields = struct_schema.get_where_properties_to_generate()?;

    let mut result = Vec::new();

    for (struct_name, models) in where_fields {
        let has_reference = models.iter().any(|model| model.attr.as_str);

        let mut fields = Vec::new();

        for model in models {
            if let Some(operator_from) = model.attr.operator_from.as_ref() {
                let operator_from = operator_from.as_str();
                fields.push(quote::quote! {
                    #[operator(#operator_from)]
                });

                let overridden_column_name = model.prop.get_db_column_name()?;
                let overridden_column_name = overridden_column_name.get_overridden_column_name();

                model
                    .prop
                    .fill_attributes(&mut fields, Some(overridden_column_name))?;

                push_field(&mut fields, &model, Some("_from"));

                if let Some(operator_to) = model.attr.operator_to.as_ref() {
                    let operator_to = operator_to.as_str();
                    fields.push(quote::quote! {
                        #[operator(#operator_to)]
                    });

                    let overridden_column_name = model.prop.get_db_column_name()?;
                    let overridden_column_name =
                        overridden_column_name.get_overridden_column_name();

                    model
                        .prop
                        .fill_attributes(&mut fields, Some(overridden_column_name))?;

                    push_field(&mut fields, &model, Some("_to"));
                }
            } else {
                if let Some(operator) = model.attr.operator.as_ref() {
                    let operator = operator.as_str();
                    fields.push(quote::quote! {
                        #[operator(#operator)]
                    })
                }

                model.prop.fill_attributes(&mut fields, None)?;

                push_field(&mut fields, &model, None);
            }

            if let Some(field_name) = model.attr.limit.as_ref() {
                let field_name = TokenStream::from_str(field_name.as_str()).unwrap();

                fields.push(quote::quote! {
                    #[limit]
                    pub #field_name: usize,
                })
            }
        }

        generate_struct(&mut result, struct_name.as_str(), has_reference, &fields);
    }

    let result = quote::quote! {
        #(#result)*
    };

    Ok(result)
}

fn generate_struct(
    result: &mut Vec<TokenStream>,
    struct_name: &str,
    has_reference: bool,
    fields: &[TokenStream],
) {
    let struct_name = TokenStream::from_str(struct_name).unwrap();

    if has_reference {
        result.push(quote::quote! {
            #[derive(my_postgres::macros::WhereDbModel)]
            pub struct #struct_name<'s>{
                #(#fields)*
            }
        });
    } else {
        result.push(quote::quote! {
            #[derive(my_postgres::macros::WhereDbModel)]
            pub struct #struct_name{
                #(#fields)*
            }
        });
    }
}

fn push_field(
    fields: &mut Vec<TokenStream>,
    model: &GenerateAdditionalWhereStruct,
    add_suffix: Option<&'static str>,
) {
    let mut ty = if model.attr.as_str {
        "&'s str".to_string()
    } else {
        model.prop.ty.get_token_stream().to_string()
    };

    if model.attr.as_vec {
        ty = format!("Vec<{}>", ty);
    }

    if model.attr.as_option {
        ty = format!("Option<{}>", ty);
    }

    let ty = TokenStream::from_str(ty.as_str()).unwrap();

    let field_name = if let Some(add_suffix) = add_suffix {
        TokenStream::from_str(format!("{}{}", model.prop.name.as_str(), add_suffix).as_str())
            .unwrap()
    } else {
        TokenStream::from_str(model.prop.name.as_str()).unwrap()
    };

    if model.attr.ignore_if_none {
        fields.push(quote::quote!(#[ignore_if_none]));
    }

    fields.push(quote::quote! {
        pub #field_name: #ty,
    });
}
