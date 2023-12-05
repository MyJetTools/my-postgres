use quote::quote;
use types_reader::{StructProperty, TypeName};

use crate::where_fields::WhereFields;

pub fn generate(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let type_name: TypeName = ast.try_into()?;

    let src_fields = StructProperty::read(ast)?;

    let src_fields = crate::postgres_struct_ext::filter_fields(src_fields)?;

    let where_fields = WhereFields::new(src_fields.as_slice());

    let result = where_fields.generate_implementation(&type_name)?;

    Ok(quote! {
        #result
    }
    .into())
}
