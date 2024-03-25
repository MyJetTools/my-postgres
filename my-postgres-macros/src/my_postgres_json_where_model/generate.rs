use quote::quote;
use types_reader::StructProperty;

use crate::postgres_struct_ext::PostgresStructPropertyExt;

pub fn generate(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let ident = &ast.ident;
    let src_fields = StructProperty::read(ast)?;

    let where_fields = generate_json_where_fields(&src_fields)?;

    let impl_where_value_provider =
        crate::where_value_provider::render_where_value_provider(&ident, || {
            quote::quote! {
         
                 let mut json_prefix = if let Some(full_condition) = &full_where_condition {
                    if full_condition.condition_no>0{
                        sql.push_str(" AND ");
                    }

                    let mut json_prefix = full_condition.json_prefix.clone();
                    json_prefix.push(full_condition.column_name);
                    json_prefix
                }else{
                    vec![]
                };
                #where_fields

                true
            }
        });

    let result = quote! {
        #impl_where_value_provider

    }
    .into();

    Ok(result)
}

fn generate_json_where_fields(
    src_fields: &Vec<StructProperty>,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut lines = Vec::new();

    lines.push(quote::quote!(let mut condition_no = 0;));

    if lines.len() > 0 {
        lines.push(quote::quote!(sql.push('(');));
    }

    for src_field in src_fields {
        let prop_name_ident = src_field.get_field_name_ident();
        let db_column_name = src_field.get_db_column_name()?;
        let metadata = src_field.get_field_metadata()?;

        let inside_json = src_field.inside_json()?;

        let where_condition = crate::where_fields::render_full_where_condition(
            &db_column_name,
            Some("json_prefix"),
            inside_json,
        );

        if src_field.ty.is_option() {
            lines.push(quote::quote! {
                if self.#prop_name_ident.fill_where_value(#where_condition, sql, params, &#metadata)
                {
                    condition_no+=1;
                }

            });
        } else {
            lines.push(quote::quote! {
                if self.#prop_name_ident.fill_where_value(#where_condition, sql, params, &#metadata){
                    condition_no+=1;
                }
                
            });
        }
    }

    if lines.len() > 0 {
        lines.push(quote::quote!(sql.push(')');));
    }

    Ok(quote::quote!(#(#lines)*))
}
