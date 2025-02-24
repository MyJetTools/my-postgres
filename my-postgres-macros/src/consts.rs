pub fn render_fn_fill_select_part_as_field() -> proc_macro2::TokenStream {
    let db_field_type = crate::render_impl::get_column_type_as_parameter();

    quote::quote! {
        pub fn fill_select_part(sql: &mut  my_postgres::sql::SelectBuilder,  column_name: #db_field_type,  metadata: &Option<my_postgres::SqlValueMetadata>) {
          sql.push(my_postgres::sql::SelectFieldValue::create_as_field(column_name, metadata));
        }
    }
}

pub fn render_fn_from_db_row_with_transformation() -> proc_macro2::TokenStream {
    quote::quote! {

        let mut db_column_name = String::new();
        my_postgres::utils::fill_adjusted_column_name(column_name.db_column_name, &mut db_column_name);
        let str_value: String = row.get(db_column_name.as_str());

        Self::from_str(str_value.as_str())
    }
}

pub fn render_fn_from_db_row_opt_with_transformation() -> proc_macro2::TokenStream {
    quote::quote! {

        let mut db_column_name = String::new();
        my_postgres::utils::fill_adjusted_column_name(column_name.db_column_name, &mut db_column_name);
        let str_value: String = row.get(db_column_name.as_str());

        let str_value: Option<String> = row.get(db_column_name.as_str());
        let str_value = str_value.as_ref()?;

        let result = Self::from_str(str_value);
        Some(result)
    }
}
