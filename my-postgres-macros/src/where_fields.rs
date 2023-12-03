use std::str::FromStr;

use types_reader::StructProperty;

use crate::{postgres_struct_ext::PostgresStructPropertyExt, struct_name::StructName};

pub struct WhereFields<'s> {
    pub limit: Option<&'s StructProperty<'s>>,
    pub offset: Option<&'s StructProperty<'s>>,
    pub where_fields: Vec<&'s StructProperty<'s>>,
}

impl<'s> WhereFields<'s> {
    pub fn new(src_fields: &'s [StructProperty<'s>]) -> Self {
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

        Self {
            limit,
            offset,
            where_fields: other_fields,
        }
    }

    pub fn generate_implementation(
        &self,
        type_name: StructName,
    ) -> Result<proc_macro2::TokenStream, syn::Error> {
        let struct_name = type_name.get_struct_name();

        let limit = self.generate_limit_fn();

        let offset = self.generate_offset_fn();

        let has_conditions_fn = self.generate_has_conditions_fn();

        let where_data = self.fn_fill_where_content()?;

        let generics = type_name.get_generic();

        let result = quote::quote! {
           impl #generics my_postgres::sql_where::SqlWhereModel for #struct_name{
            fn fill_where_component(&self, sql: &mut String, params: &mut my_postgres::sql::SqlValues){
                use my_postgres::SqlWhereValueProvider;
                #where_data
            }
            #limit
            #offset
            #has_conditions_fn
           }
        };

        Ok(result.into())
    }

    pub fn generate_limit_fn(&self) -> proc_macro2::TokenStream {
        if let Some(limit) = &self.limit {
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

    pub fn generate_offset_fn(&self) -> proc_macro2::TokenStream {
        if let Some(offset) = &self.offset {
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
    pub fn generate_has_conditions_fn(&self) -> proc_macro2::TokenStream {
        let has_fields = self.where_fields.len() > 0;

        quote::quote! {
            fn has_conditions(&self) -> bool{
                #has_fields
            }
        }
    }

    pub fn fn_fill_where_content(&self) -> Result<proc_macro2::TokenStream, syn::Error> {
        let mut lines = Vec::new();

        lines.push(quote::quote! {
            let mut condition_no = 0;
        });

        for prop in &self.where_fields {
            let prop_name_ident = prop.get_field_name_ident();
            let db_column_name = prop.get_db_column_name_as_string()?;
            let metadata = prop.get_field_metadata()?;

            let ignore_if_none = prop.has_ignore_if_none_attr();

            let where_condition = render_full_where_condition(db_column_name, None);

            if prop.ty.is_option() {
                if ignore_if_none {
                    lines.push(quote::quote! {
                        if let Some(value) = &self.#prop_name_ident{
                            value.fill_where_value(#where_condition, sql, params, &#metadata);
                            condition_no+=1;
                        }
                    });
                } else {
                    lines.push(quote::quote! {
                        if let Some(value) = &self.#prop_name_ident{
                            value.fill_where_value(#where_condition, sql, params, &#metadata);
                        }
                        else{
                            if condition_no>0{
                                sql.push_str(" AND ");
                            }
                            sql.push_str(#db_column_name);
                            sql.push_str(" IS NULL");
                        }
                        condition_no+=1;
                    });
                }
            } else {
                lines.push(quote::quote! {
                    self.#prop_name_ident.fill_where_value(#where_condition, sql, params, &#metadata);
                    condition_no+=1;
                });
            }
        }

        let result = quote::quote! {
            #(#lines)*
        };

        Ok(result)
    }
}

pub fn render_full_where_condition(
    db_column_name: &str,
    json_column_name: Option<&str>,
) -> proc_macro2::TokenStream {
    let json_column_name = if let Some(json_column_name) = json_column_name {
        let json_column_name = proc_macro2::TokenStream::from_str(json_column_name).unwrap();
        quote::quote!(Some(#json_column_name))
    } else {
        quote::quote!(None)
    };

    quote::quote! {
        Some(my_postgres::RenderFullWhereCondition{
            column_name: #db_column_name,
            condition_no,
            json_prefix: #json_column_name
        })
    }
}