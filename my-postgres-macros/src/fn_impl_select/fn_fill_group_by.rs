use quote::quote;

use crate::{
    postgres_struct_ext::PostgresStructPropertyExt, postgres_struct_schema::PostgresStructSchema,
};

pub fn get_group_by_fields<'s>(
    fields: &'s impl PostgresStructSchema<'s>,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let fields = fields.get_fields();
    let mut group_by_columns = Vec::with_capacity(fields.len());

    for prop in fields {
        if prop.attrs.has_attr("group_by") {
            group_by_columns.push(prop.get_db_column_name()?);
            continue;
        }
    }

    if group_by_columns.is_empty() {
        return Ok(quote! { None }.into());
    }

    let mut group_by_str = String::new();

    group_by_str.push_str(" GROUP BY");
    for group_by_column in group_by_columns {
        group_by_str.push(' ');
        group_by_str.push_str(group_by_column.as_str());
    }

    Ok(quote! { Some(#group_by_str) }.into())
}
