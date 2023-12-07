use proc_macro::TokenStream;
use types_reader::{StructureSchema, TypeName};

use super::update_fields::UpdateFields;

pub fn generate(ast: &syn::DeriveInput) -> Result<TokenStream, syn::Error> {
    let type_name: TypeName = ast.try_into()?;

    let fields = StructureSchema::new(ast)?;

    let update_fields = UpdateFields::new_from_update_model(&fields);

    let main_model = super::generate_derive_model(&type_name, update_fields)?;

    Ok(main_model.into())
}
