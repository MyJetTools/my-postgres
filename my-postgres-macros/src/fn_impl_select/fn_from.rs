use crate::{
    postgres_struct_ext::PostgresStructPropertyExt, postgres_struct_schema::PostgresStructSchema,
};
use quote::quote;
use types_reader::PropertyType;

pub fn fn_from<'s>(
    fields: &'s impl PostgresStructSchema<'s>,
) -> Result<Vec<proc_macro2::TokenStream>, syn::Error> {
    let fields = fields.get_fields();
    let mut result = Vec::with_capacity(fields.len());

    for field in fields {
        let name_ident = field.get_field_name_ident();

        let force_cast_db_type = field.get_force_cast_db_type();
        let db_column_name = field
            .get_db_column_name()?
            .to_column_name_token(force_cast_db_type);

        let metadata = field.get_field_metadata()?;

        let type_ident = field.get_ty_to_invoke_static_methods()?;

        let reading = if let PropertyType::OptionOf(_) = &field.ty {
            quote!(#type_ident::from_db_row_opt(db_row, #db_column_name, &#metadata))
        } else {
            quote!(#type_ident::from_db_row(db_row, #db_column_name, &#metadata))
        };

        result.push(quote! {
            #name_ident: #reading,
        });
    }

    Ok(result)
}
