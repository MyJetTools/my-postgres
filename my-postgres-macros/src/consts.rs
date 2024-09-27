pub fn render_fn_fill_select_part_as_field() -> proc_macro2::TokenStream {
    let db_field_type = crate::render_impl::get_column_type_as_parameter();

    quote::quote! {
        pub fn fill_select_part(sql: &mut  my_postgres::sql::SelectBuilder,  column_name: #db_field_type,  metadata: &Option<my_postgres::SqlValueMetadata>) {
          sql.push(my_postgres::sql::SelectFieldValue::create_as_field(column_name, metadata));
        }
    }
}
