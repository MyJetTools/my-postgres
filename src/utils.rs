pub fn compile_enum_with_model(case: &str, volume: &str) -> String {
    let mut json_writer = my_json::json_writer::JsonObjectWriter::new();

    json_writer.write_value("case", case);
    json_writer.write_raw_value("model", volume.as_bytes());

    String::from_utf8(json_writer.build()).unwrap()
}
