use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum TokenType {
    Keyword,
    Identifier,
    StringLiteral,
    Number,
    Operator,
    Punctuation,
    Whitespace,
    Comment,
    TemplateVariable,
    TemplateBlock,
    EOF,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub value: String,
    pub start: usize,
}

#[derive(Debug)]
pub struct TokenizerResult {
    pub tokens: Vec<Token>,
}

#[derive(Debug)]
pub struct Tokenizer<'a> {
    input: &'a str,
    position: usize,
    keywords: HashMap<&'a str, bool>,
}

impl Tokenizer<'_> {
    pub fn new(input: &str) -> Self {
        let mut keywords = HashMap::new();
        // Add SQL keywords
        for kw in &[
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
            "DISTINCT", "TRUE", "FALSE", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS", "ON",
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "UNION", "GROUP", "BY", "ORDER",
            "HAVING", "LIMIT", "OFFSET", "WITH", "CTE", "CASE", "WHEN", "THEN", "ELSE", "END",
            "COPY", "INTO", "VALUES", "SET"
        ] {
            keywords.insert(*kw, true);
        }

        Self {
            input,
            position: 0,
            keywords,
        }
    }

    pub fn tokenize(&mut self) -> TokenizerResult {
        let mut tokens = Vec::new();
        while self.position < self.input.len() {
            match self.current_char() {
                Some('{') if self.peek() == Some('{') => {
                    tokens.push(self.tokenize_template_variable());
                }
                Some('{') if self.peek() == Some('%') => {
                    tokens.push(self.tokenize_template_block());
                }
                Some(c) if c.is_whitespace() => {
                    tokens.push(self.tokenize_whitespace());
                }
                Some(c) if c.is_alphabetic() || c == '_' => {
                    tokens.push(self.tokenize_identifier_or_keyword());
                }
                Some(c) if c.is_digit(10) => {
                    tokens.push(self.tokenize_number());
                }
                Some('"') | Some('\'') => {
                    tokens.push(self.tokenize_string());
                }
                Some('-') if self.peek() == Some('-') => {
                    tokens.push(self.tokenize_comment());
                }
                Some('/') if self.peek() == Some('*') => {
                    tokens.push(self.tokenize_multiline_comment());
                }
                Some(':') if self.peek() == Some(':') => {
                    tokens.push(self.tokenize_operator());
                }
                Some(':') => {
                    let start = self.position;
                    self.advance();
                    let end = self.position;
                    tokens.push(Token {
                        token_type: TokenType::Punctuation,
                        value: self.input[start..end].to_string(),
                        start,
                    });
                }
                Some(c) if "+-*/=<>!@".contains(c) => {
                    tokens.push(self.tokenize_operator());
                }
                Some(c) if "(),.;".contains(c) => {
                    let start = self.position;
                    self.advance();
                    let end = self.position;
                    tokens.push(Token {
                        token_type: TokenType::Punctuation,
                        value: self.input[start..end].to_string(),
                        start,
                    });
                }
                _ => {
                    // Unknown token, skip or error
                    self.advance();
                }
            }
        }
        let eof_start = self.position;
        tokens.push(Token {
            token_type: TokenType::EOF,
            value: String::new(),
            start: eof_start,
        });
        TokenizerResult { tokens }
    }

    fn current_char(&self) -> Option<char> {
        self.input.chars().nth(self.position)
    }

    fn peek(&self) -> Option<char> {
        self.input.chars().nth(self.position + 1)
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn tokenize_whitespace(&mut self) -> Token {
        let start = self.position;
        while self.current_char().map_or(false, |c| c.is_whitespace()) {
            self.advance();
        }
        let end = self.position;
        Token {
            token_type: TokenType::Whitespace,
            value: self.input[start..end].to_string(),
            start,
        }
    }

    fn tokenize_identifier_or_keyword(&mut self) -> Token {
        let start = self.position;
        while self.current_char().map_or(false, |c| c.is_alphanumeric() || c == '_') {
            self.advance();
        }
        let end = self.position;
        let ident = &self.input[start..end];

        // Check if it's a keyword (case-insensitive comparison)
        let ident_upper = ident.to_uppercase();
        if self.keywords.contains_key(ident_upper.as_str()) {
            Token {
                token_type: TokenType::Keyword,
                value: ident.to_string(),
                start,
            }
        } else {
            Token {
                token_type: TokenType::Identifier,
                value: ident.to_string(),
                start,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestCase {
        name: String,
        sql: String,
        tokens: Vec<TestToken>,
    }

    #[derive(Debug, Deserialize)]
    struct TestToken {
        #[serde(rename = "type")]
        token_type: Option<String>,
        value: Option<String>,
        position: Option<usize>,
    }

    #[derive(Debug, Deserialize)]
    struct TestFile {
        file: Vec<TestCase>,
    }

    fn load_test_file(path: &str) -> TestFile {
        let content = fs::read_to_string(path).expect("Failed to read test file");
        serde_yaml::from_str(&content).expect("Failed to parse YAML")
    }

    fn run_yaml_tests(test_file_path: &str) {
        let test_file = load_test_file(test_file_path);

        for test_case in test_file.file {
            println!("Running: {}", test_case.name);

            let mut tokenizer = Tokenizer::new(&test_case.sql);
            let result = tokenizer.tokenize();
            let tokens = &result.tokens;

            // Filter out EOF and whitespace tokens for comparison (unless explicitly expected)
            let actual_tokens: Vec<&Token> = tokens.iter()
                .filter(|t| t.token_type != TokenType::EOF && t.token_type != TokenType::Whitespace)
                .collect();

            // Check each expected token
            for (i, expected) in test_case.tokens.iter().enumerate() {
                if i >= actual_tokens.len() {
                    panic!("Test '{}' failed: Expected {} tokens, got {}", test_case.name, test_case.tokens.len(), actual_tokens.len());
                }

                let actual = &actual_tokens[i];

                // Check token type if specified
                if let Some(ref expected_type) = expected.token_type {
                    let actual_type = match actual.token_type {
                        TokenType::Keyword => "Keyword",
                        TokenType::Identifier => "Identifier",
                        TokenType::StringLiteral => "StringLiteral",
                        TokenType::Number => "Number",
                        TokenType::Operator => "Operator",
                        TokenType::Punctuation => "Punctuation",
                        TokenType::Whitespace => "Whitespace",
                        TokenType::Comment => "Comment",
                        TokenType::TemplateVariable => "TemplateVariable",
                        TokenType::TemplateBlock => "TemplateBlock",
                        TokenType::EOF => "EOF",
                    };

                    if expected_type != actual_type {
                        panic!("Test '{}' failed at token {}: Expected type '{}', got '{}'",
                               test_case.name, i, expected_type, actual_type);
                    }
                }

                // Check value if specified
                if let Some(ref expected_value) = expected.value {
                    if expected_value != actual.value {
                        panic!("Test '{}' failed at token {}: Expected value '{}', got '{}'",
                               test_case.name, i, expected_value, actual.value);
                    }
                }

                // Check position if specified
                if let Some(expected_pos) = expected.position {
                    if expected_pos != actual.start {
                        panic!("Test '{}' failed at token {}: Expected position {}, got {}",
                               test_case.name, i, expected_pos, actual.start);
                    }
                }
            }

            println!("  ✓ Passed");
        }
    }

    fn run_all_yaml_tests() {
        let test_dir = std::path::Path::new("test/fixtures/tokenizer");

        if !test_dir.exists() {
            println!("No tokenizer tests found (directory doesn't exist)");
            return;
        }

        let entries = fs::read_dir(test_dir).expect("Failed to read test fixtures directory");

        for entry in entries {
            let entry = entry.expect("Failed to read directory entry");
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("yml") {
                let file_name = path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");

                println!("\n=== Running tests from {} ===", file_name);
                run_yaml_tests(path.to_str().unwrap());
            }
        }
    }

    #[test]
    fn test_all_tokenization() {
        run_all_yaml_tests();
    }
}
