use sioc_macros::Event;

#[derive(Event, Debug, PartialEq)]
pub enum TestEvents {
    #[sioc(event = "ping")]
    Ping,
    #[sioc(event = "message")]
    Message(String),
    #[sioc(event = "complex")]
    Complex { a: i32, b: String },
}

#[derive(Event, Debug, PartialEq)]
#[sioc(event = "struct_unit")]
pub struct UnitStruct;

#[derive(Event, Debug, PartialEq)]
#[sioc(event = "new_type")]
pub struct MyWrapper(pub String, pub i32);

#[derive(Event, Debug, PartialEq)]
#[sioc(event = "struct_event")]
pub struct MyStruct {
    pub foo: String,
    pub bar: i32,
}
