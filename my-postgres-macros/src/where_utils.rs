use types_reader::StructProperty;

pub fn get_limit_and_offset_fields<'s>(
    src_fields: Vec<StructProperty<'s>>,
) -> (
    Option<StructProperty<'s>>,
    Option<StructProperty<'s>>,
    Vec<StructProperty<'s>>,
) {
    let mut limit = None;
    let mut offset = None;
    let mut other_fields = Vec::new();
    for struct_prop in src_fields {
        if struct_prop.attrs.has_attr("limit") {
            limit = Some(struct_prop);
        } else if struct_prop.attrs.has_attr("offset") {
            offset = Some(struct_prop);
        } else {
            other_fields.push(struct_prop);
        }
    }

    (limit, offset, other_fields)
}

pub fn generate_limit_fn(limit: Option<StructProperty>) -> proc_macro2::TokenStream {
    if let Some(limit) = &limit {
        let name = limit.get_field_name_ident();
        quote::quote! {
            fn get_limit(&self) -> Option<usize> {
                self.#name.into()
            }
        }
        .into()
    } else {
        quote::quote! {
            fn get_limit(&self) -> Option<usize> {
                None
            }
        }
        .into()
    }
}

pub fn generate_offset_fn(offset: Option<StructProperty>) -> proc_macro2::TokenStream {
    if let Some(offset) = &offset {
        let name = offset.get_field_name_ident();
        quote::quote! {
            fn get_offset(&self) -> Option<usize> {
                self.#name.into()
            }
        }
        .into()
    } else {
        quote::quote! {
            fn get_offset(&self) -> Option<usize> {
                None
            }
        }
        .into()
    }
}
