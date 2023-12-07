use quote::quote;
use types_reader::StructureSchema;

use crate::where_fields::WhereFields;

pub fn generate(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let structure_schema = StructureSchema::new(ast)?;

    let where_fields = WhereFields::new(&structure_schema);

    let result = where_fields.generate_implementation(&structure_schema.name)?;

    Ok(quote! {
        #result
    }
    .into())
}
