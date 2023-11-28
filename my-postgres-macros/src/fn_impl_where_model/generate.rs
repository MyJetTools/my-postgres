use proc_macro2::TokenStream;
use quote::quote;
use types_reader::{StructProperty, TypeName};

use crate::struct_name::StructName;

pub fn generate(ast: &syn::DeriveInput) -> Result<proc_macro::TokenStream, syn::Error> {
    let type_name = TypeName::new(ast);

    let src_fields = StructProperty::read(ast)?;

    let src_fields = crate::postgres_struct_ext::filter_fields(src_fields)?;

    let (limit, offset, fields) = crate::where_utils::get_limit_and_offset_fields(src_fields);

    let result = generate_implementation(
        StructName::TypeName(&type_name),
        fields.iter(),
        limit,
        offset,
    )?;

    Ok(quote! {
        #result
    }
    .into())
}

pub fn generate_implementation<'s>(
    type_name: StructName<'s>,
    fields: impl Iterator<Item = &'s StructProperty<'s>>,
    limit: Option<StructProperty>,
    offset: Option<StructProperty>,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let struct_name = type_name.get_struct_name();

    let limit = crate::where_utils::generate_limit_fn(limit);

    let offset: TokenStream = crate::where_utils::generate_offset_fn(offset);

    let where_data = super::fn_fill_where::fn_fill_where(fields)?;

    let generics = type_name.get_generic();

    let result = quote! {
       impl #generics my_postgres::sql_where::SqlWhereModel for #struct_name{
        fn get_where_field_name_data(&self, no: usize) -> Option<my_postgres::sql_where::WhereFieldData>{
            #where_data
        }
        #limit
        #offset
       }
    };

    Ok(result.into())
}
