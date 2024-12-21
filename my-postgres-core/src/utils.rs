use my_json::{json_reader::JsonValueRef, json_writer::RawJsonObject};

pub fn compile_enum_with_model(case: &str, volume: &str) -> String {
    let mut json_writer = my_json::json_writer::JsonObjectWriter::new();

    json_writer.write("case", case);
    json_writer.write("model", RawJsonObject::AsStr(volume));

    json_writer.build()
}

pub fn get_case_and_model<'s>(
    first_line_reader: &'s my_json::json_reader::JsonFirstLineIterator<'s>,
) -> (JsonValueRef<'s>, JsonValueRef<'s>) {
    let mut case = None;
    let mut model = None;

    while let Some(itm) = first_line_reader.get_next() {
        let (name, value) = itm.unwrap();
        match name.as_str().unwrap().as_str() {
            "case" => case = Some(value),
            "model" => model = Some(value),
            _ => {}
        }
    }

    if case.is_none() {
        panic!("Can't find case in {:?}", first_line_reader.as_str());
    }

    if model.is_none() {
        panic!("Can't find model in {:?}", first_line_reader.as_str());
    }

    (case.unwrap(), model.unwrap())
}

/*
pub struct CaseAndModelValue {
    case: JsonKeyValue,
    model: JsonKeyValue,
}

impl CaseAndModelValue {
    pub fn get_case<'s>(&self, src: &'s str) -> StrOrString<'s> {
        let result = self.case.value.as_str(&src).unwrap();
        result
    }

    pub fn get_model<'s>(&self, src: &'s str) -> &'s str {
        let result = self.model.value.as_raw_str(&src).unwrap();

        result
    }
}
 */
