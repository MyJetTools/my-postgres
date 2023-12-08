use types_reader::{EnumCase, MacrosAttribute};

use crate::attributes::{DefaultValueAttribute, EnumCaseAttribute};

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
        if enum_case.attrs.has_attr(DefaultValueAttribute::NAME) {
            let enum_case_attr: Option<EnumCaseAttribute> = enum_case.try_get_attribute()?;

            if let Some(attr) = enum_case_attr {
                if let Some(value) = attr.value {
                    return Ok(quote::quote! {
                    pub fn get_default_value()->&'static str{
                      #value
                    }
                    });
                }
            }

            let value = enum_case.get_name_ident().to_string();

            return Ok(quote::quote! {
            pub fn get_default_value()->&'static str{
              #value
            }
            });
        }
    }

    Ok(quote::quote!())
}
