#[derive(Debug, Clone)]
pub struct Dialect {
    pub string_char: char,
    pub identifier_char: char,
}

impl Default for Dialect {
    fn default() -> Self {
        Self {
            string_char: '\'',
            identifier_char: '"',
        }
    }
}
