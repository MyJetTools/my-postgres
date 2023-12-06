use std::collections::HashMap;

use rust_extensions::slice_of_u8_utils::SliceOfU8Ext;
use types_reader::{StructProperty, TokensObject, TypeName};

use crate::{postgres_struct_ext::PostgresStructPropertyExt, where_fields::WhereFields};

pub fn generate_where_raw_model<'s>(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let attr: proc_macro2::TokenStream = attr.into();
    let params_list = TokensObject::new(attr.into())?;

    let sql = params_list.get_from_single_or_named("sql")?;

    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    let type_name: TypeName = (&ast).try_into()?;

    let src_fields = StructProperty::read(&ast)?;

    let where_fields = WhereFields::new(src_fields.as_slice());

    let generate_limit_fn = where_fields.generate_limit_fn();
    let generate_offset_fn = where_fields.generate_offset_fn();

    let mut src_as_hashmap = HashMap::new();

    for field in where_fields.where_fields {
        src_as_hashmap.insert(field.name.to_string(), field);
    }

    let tokens = scan_sql_for_placeholders(sql.try_into()?);

    let mut content_to_render = Vec::new();

    for token in tokens {
        match token {
            SqlTransformToken::PlaceHolder(property_name) => {
                let property = src_as_hashmap.get(property_name);

                if property.is_none() {
                    panic!("Property not found: {}", property_name)
                }

                let property = property.unwrap();

                let name = property.get_field_name_ident();
                let meta_data = property.get_field_metadata()?;

                content_to_render.push(quote::quote!(
                   self.#name.fill_where_value(None, sql, params, &#meta_data);

                ));
            }
            SqlTransformToken::RawContent(content) => {
                content_to_render.push(quote::quote!(
                    sql.push_str(#content);
                ));
            }
        }
    }

    let impl_where_model = crate::render_impl::impl_sql_where_model(
        &type_name,
        {
            quote::quote! {
                use my_postgres::SqlWhereValueProvider;
                #(#content_to_render)*
            }
        },
        quote::quote!(true),
        generate_limit_fn,
        generate_offset_fn,
    );

    Ok(quote::quote! {
        #ast
        #impl_where_model
    })
}

fn scan_sql_for_placeholders<'s>(sql: &'s str) -> Vec<SqlTransformToken<'s>> {
    let mut pos_from = 0usize;

    let as_bytes = sql.as_bytes();

    let mut tokens = Vec::new();

    while let Some(place_holder_start_position) =
        as_bytes.find_sequence_pos("${".as_bytes(), pos_from)
    {
        let content =
            std::str::from_utf8(&as_bytes[pos_from..place_holder_start_position]).unwrap();

        tokens.push(SqlTransformToken::RawContent(content));

        let place_holder_end_position =
            as_bytes.find_sequence_pos("}".as_bytes(), place_holder_start_position);

        if place_holder_end_position.is_none() {
            break;
        }

        let place_holder_end_position = place_holder_end_position.unwrap();

        let field_name = std::str::from_utf8(
            &as_bytes[place_holder_start_position + 2..place_holder_end_position],
        )
        .unwrap();

        tokens.push(SqlTransformToken::PlaceHolder(field_name));

        pos_from = place_holder_end_position + 1;
    }

    if pos_from < sql.len() {
        let content = std::str::from_utf8(&as_bytes[pos_from..sql.len()]).unwrap();

        tokens.push(SqlTransformToken::RawContent(content))
    }

    tokens
}

#[derive(Debug)]
pub enum SqlTransformToken<'s> {
    RawContent(&'s str),
    PlaceHolder(&'s str),
}
