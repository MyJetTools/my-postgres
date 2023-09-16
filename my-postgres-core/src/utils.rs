pub fn compile_enum_with_model(case: &str, volume: &str) -> String {
    let mut json_writer = my_json::json_writer::JsonObjectWriter::new();

    json_writer.write_value("case", case);
    json_writer.write_raw_value("model", volume.as_bytes());

    String::from_utf8(json_writer.build()).unwrap()
}

pub fn get_case_and_model<'s>(value: &'s str) -> (&'s str, &'s str) {
    let mut case = None;
    let mut model = None;

    for itm in my_json::json_reader::JsonFirstLineReader::new(value.as_bytes()) {
        let itm = itm.unwrap();

        match itm.get_name().unwrap() {
            "case" => case = Some(itm.get_value().unwrap().as_str().unwrap()),
            "model" => {
                model =
                    Some(std::str::from_utf8(itm.get_value().unwrap().as_bytes().unwrap()).unwrap())
            }
            _ => {}
        }
    }

    if case.is_none() {
        panic!("Can't find case in {}", value);
    }

    if model.is_none() {
        panic!("Can't find model in {}", value);
    }

    (case.unwrap(), model.unwrap())
}
