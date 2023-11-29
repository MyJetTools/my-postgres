use types_reader::EnumCase;

use crate::postgres_enum_ext::PostgresEnumExt;

pub fn render_update_value_provider_fn_body_as_json() -> proc_macro2::TokenStream {
    quote::quote! {
        let value = self.to_str();
        let index = params.push(value.into());
        my_postgres::sql::SqlUpdateValue::Json(index)
    }
}

pub fn render_select_part_as_json() -> proc_macro2::TokenStream {
    quote::quote! {
        sql.push(my_postgres::sql::SelectFieldValue::Json(field_name));
    }
}

pub fn get_default_value(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    for enum_case in enum_cases {
        if enum_case.attrs.has_attr("default_value") {
            let value = enum_case.get_case_any_string_value()?;

            return Ok(quote::quote! {
            pub fn get_default_value()->&'static str{
              #value
            }
            });
        }
    }

    Ok(quote::quote!())
}
