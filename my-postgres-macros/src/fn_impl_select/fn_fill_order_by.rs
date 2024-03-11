use crate::{
    postgres_struct_ext::PostgresStructPropertyExt, postgres_struct_schema::PostgresStructSchema,
};
use quote::quote;

pub fn fn_get_order_by_fields<'s>(
    fields: &'s impl PostgresStructSchema<'s>,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let fields = fields.get_fields();
    let mut order_by_desc = Vec::with_capacity(fields.len());
    let mut order_by = Vec::with_capacity(fields.len());

    for prop in fields {
        if prop.attrs.has_attr("order_by_desc") {
            order_by_desc.push(prop);
            continue;
        }

        if prop.attrs.has_attr("order_by") {
            order_by.push(prop);
            continue;
        }
    }

    if order_by_desc.is_empty() && order_by.is_empty() {
        return Ok(quote! { None }.into());
    }

    if !order_by_desc.is_empty() && !order_by.is_empty() {
        return Err(syn::Error::new_spanned(
            order_by_desc.get(0).unwrap().field,
            "Ether order_by_desc or order_by must be set, not both",
        ));
    }

    let mut result = String::new();
    result.push_str(" ORDER BY");

    if !order_by_desc.is_empty() {
        for (i, field) in order_by_desc.iter().enumerate() {
            if i == 0 {
                result.push(' ');
            } else {
                result.push(',');
            }

            let db_column_name = field.get_db_column_name()?;
            result.push_str(db_column_name.as_str());
        }
        result.push_str(" DESC");
    } else if !order_by.is_empty() {
        for (i, field) in order_by.iter().enumerate() {
            if i == 0 {
                result.push(' ');
            } else {
                result.push(',');
            }

            result.push_str(field.get_db_column_name()?.as_str());
        }
    }

    return Ok(quote!(Some(#result)).into());
}
