use std::collections::HashMap;

use rust_extensions::slice_of_u8_utils::SliceOfU8Ext;
use types_reader::{ParamsList, StructProperty, TypeName};

pub fn generate_where_raw_model<'s>(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let params_list = ParamsList::new(attr.into(), || None)?;

    let sql = params_list
        .get_from_single_or_named("sql")?
        .unwrap_as_string_value()?;

    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    let type_name = TypeName::new(&ast);

    let struct_name = type_name.get_type_name();

    let src_fields = StructProperty::read(&ast)?;

    let (limit, offset, fields) = crate::where_utils::get_limit_and_offset_fields(src_fields);

    let limit = crate::where_utils::generate_limit_fn(limit);

    let offset = crate::where_utils::generate_offset_fn(offset);

    let mut src_as_hashmap = HashMap::new();

    for field in fields {
        src_as_hashmap.insert(field.name.to_string(), field);
    }

    let tokens = scan_sql_for_placeholders(sql.as_str());

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

                content_to_render.push(quote::quote!(
                    my_postgres::sql_where::WhereRawData::PlaceHolder(&self.#name)
                ));
            }
            SqlTransformToken::RawContent(content) => {
                content_to_render.push(quote::quote!(
                    my_postgres::sql_where::WhereRawData::Content(#content)
                ));
            }
        }
    }

    Ok(quote::quote! {
        #ast
        impl my_postgres::sql_where::SqlWhereModel for #struct_name{
            fn get_where_field_name_data(&self, no: usize) -> Option<my_postgres::sql_where::WhereFieldData>{
                if no > 0 {
                    return None;
                }

                let data = vec![#(#content_to_render),*];
                let result = my_postgres::sql_where::WhereFieldData::Raw(data);

                Some(result)
            }

            #limit
            #offset
           }
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
