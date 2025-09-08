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
    Newline,
    Comment,
    TemplateVariable,
    TemplateBlock,
    EOF,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenizerError {
    UnmatchedToken { expected: String, found: String, position: usize },
    InvalidSyntax { message: String, position: usize },
    UnterminatedString { position: usize },
    UnterminatedComment { position: usize },
    UnterminatedTemplate { template_type: String, position: usize },
}

impl std::fmt::Display for TokenizerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TokenizerError::UnmatchedToken { expected, found, position } => {
                write!(f, "Unmatched token at position {}: expected {}, found {}", position, expected, found)
            }
            TokenizerError::InvalidSyntax { message, position } => {
                write!(f, "Invalid syntax at position {}: {}", position, message)
            }
            TokenizerError::UnterminatedString { position } => {
                write!(f, "Unterminated string literal at position {}", position)
            }
            TokenizerError::UnterminatedComment { position } => {
                write!(f, "Unterminated comment at position {}", position)
            }
            TokenizerError::UnterminatedTemplate { template_type, position } => {
                write!(f, "Unterminated {} template at position {}", template_type, position)
            }
        }
    }
}

impl std::error::Error for TokenizerError {}
#[derive(Debug, Clone, PartialEq)]
pub struct TokenPair {
    pub token_type: TokenType,
    pub loc: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub value: String,
    pub start: usize,
    pub pair: Option<TokenPair>,
}

#[derive(Debug)]
pub struct TokenizerResult {
    pub tokens: Vec<Token>,
}

#[derive(Debug)]
pub struct Tokenizer<'a> {
    input: &'a str,
    chars_iter: std::str::CharIndices<'a>,
    current_pos: Option<(usize, char)>,
    keywords: HashMap<&'a str, bool>,
    pair_stack: Vec<TokenPair>,
    tokens: Vec<Token>,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        let mut keywords = HashMap::new();
        // Add SQL keywords
        for kw in &[
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
            "DISTINCT", "TRUE", "FALSE", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS", "ON",
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "UNION", "GROUP", "BY", "ORDER",
            "HAVING", "LIMIT", "OFFSET", "WITH", "CTE", "CASE", "WHEN", "THEN", "ELSE", "END",
            "COPY", "INTO", "VALUES", "SET",
        ] {
            keywords.insert(*kw, true);
        }

        let mut chars_iter = input.char_indices();
        let current_pos = chars_iter.next();

        Self {
            input,
            chars_iter,
            current_pos,
            keywords,
            pair_stack: Vec::new(),
            tokens: Vec::new(),
        }
    }
    fn add_token(&mut self, token: Token) -> Result<(), TokenizerError> {
        let token_i = self.tokens.len();
        // Check if opening token
        let is_opening = (token.token_type == TokenType::Punctuation && (token.value == "(" || token.value == "{" || token.value == "[")) ||
                          (token.token_type == TokenType::Keyword && (token.value.to_uppercase() == "SELECT" || token.value.to_uppercase() == "CASE"));
        if is_opening {
            let pair = TokenPair {
                token_type: token.token_type.clone(),
                loc: token_i,
            };
            self.pair_stack.push(pair);
            self.tokens.push(token);
            return Ok(());
        }
        // Check if closing token
        if token.token_type == TokenType::Punctuation && token.value == ")" {
            if let Some(opening_pair) = self.pair_stack.last() {
                if opening_pair.token_type == TokenType::Punctuation && self.tokens[opening_pair.loc].value == "(" {
                    let opening_pair = self.pair_stack.pop().unwrap();
                    let open_token = &mut self.tokens[opening_pair.loc];
                    let token_with_pair = Token {
                        pair: Some(opening_pair.clone()),
                        ..token.clone()
                    };
                    open_token.pair = Some(TokenPair {
                        token_type: token.token_type.clone(),
                        loc: token_i,
                    });
                    self.tokens.push(token_with_pair);
                    return Ok(());
                } else {
                    return Err(TokenizerError::UnmatchedToken {
                        expected: "(".to_string(),
                        found: token.value,
                        position: token.start,
                    });
                }
            } else {
                return Err(TokenizerError::UnmatchedToken {
                    expected: "(".to_string(),
                    found: token.value,
                    position: token.start,
                });
            }
        } else if token.token_type == TokenType::Punctuation && token.value == "}" {
            if let Some(opening_pair) = self.pair_stack.last() {
                if opening_pair.token_type == TokenType::Punctuation && self.tokens[opening_pair.loc].value == "{" {
                    let opening_pair = self.pair_stack.pop().unwrap();
                    let open_token = &mut self.tokens[opening_pair.loc];
                    let token_with_pair = Token {
                        pair: Some(opening_pair.clone()),
                        ..token.clone()
                    };
                    open_token.pair = Some(TokenPair {
                        token_type: token.token_type.clone(),
                        loc: token_i,
                    });
                    self.tokens.push(token_with_pair);
                    return Ok(());
                } else {
                    return Err(TokenizerError::UnmatchedToken {
                        expected: "{".to_string(),
                        found: token.value,
                        position: token.start,
                    });
                }
            } else {
                return Err(TokenizerError::UnmatchedToken {
                    expected: "{".to_string(),
                    found: token.value,
                    position: token.start,
                });
            }
        } else if token.token_type == TokenType::Punctuation && token.value == "]" {
            if let Some(opening_pair) = self.pair_stack.last() {
                if opening_pair.token_type == TokenType::Punctuation && self.tokens[opening_pair.loc].value == "[" {
                    let opening_pair = self.pair_stack.pop().unwrap();
                    let open_token = &mut self.tokens[opening_pair.loc];
                    let token_with_pair = Token {
                        pair: Some(opening_pair.clone()),
                        ..token.clone()
                    };
                    open_token.pair = Some(TokenPair {
                        token_type: token.token_type.clone(),
                        loc: token_i,
                    });
                    self.tokens.push(token_with_pair);
                    return Ok(());
                } else {
                    return Err(TokenizerError::UnmatchedToken {
                        expected: "[".to_string(),
                        found: token.value,
                        position: token.start,
                    });
                }
            } else {
                return Err(TokenizerError::UnmatchedToken {
                    expected: "[".to_string(),
                    found: token.value,
                    position: token.start,
                });
            }
        } else if token.token_type == TokenType::Keyword && token.value.to_uppercase() == "FROM" {
            if let Some(opening_pair) = self.pair_stack.last() {
                if opening_pair.token_type == TokenType::Keyword && self.tokens[opening_pair.loc].value.to_uppercase() == "SELECT" {
                    let opening_pair = self.pair_stack.pop().unwrap();
                    let open_token = &mut self.tokens[opening_pair.loc];
                    let token_with_pair = Token {
                        pair: Some(opening_pair.clone()),
                        ..token.clone()
                    };
                    open_token.pair = Some(TokenPair {
                        token_type: token.token_type.clone(),
                        loc: token_i,
                    });
                    self.tokens.push(token_with_pair);
                    return Ok(());
                }
            }
            // If no SELECT on stack, just add FROM normally
            self.tokens.push(token);
            return Ok(())
        } else if token.token_type == TokenType::Keyword && token.value.to_uppercase() == "END" {
            if let Some(opening_pair) = self.pair_stack.last() {
                if opening_pair.token_type == TokenType::Keyword && self.tokens[opening_pair.loc].value.to_uppercase() == "CASE" {
                    let opening_pair = self.pair_stack.pop().unwrap();
                    let open_token = &mut self.tokens[opening_pair.loc];
                    let token_with_pair = Token {
                        pair: Some(opening_pair.clone()),
                        ..token.clone()
                    };
                    open_token.pair = Some(TokenPair {
                        token_type: token.token_type.clone(),
                        loc: token_i,
                    });
                    self.tokens.push(token_with_pair);
                    return Ok(());
                } else {
                    return Err(TokenizerError::UnmatchedToken {
                        expected: "CASE".to_string(),
                        found: token.value,
                        position: token.start,
                    });
                }
            } else {
                return Err(TokenizerError::UnmatchedToken {
                    expected: "CASE".to_string(),
                    found: token.value,
                    position: token.start,
                });
            }
        }
        self.tokens.push(token);
        Ok(())
    }

    pub fn tokenize(&mut self) -> Result<TokenizerResult, TokenizerError> {
        while self.current_pos.is_some() {
            match self.current_char() {
                Some('{') if self.peek() == Some('{') => {
                    let token = self.tokenize_template_variable()?;
                    self.add_token(token)?;
                }
                Some('{') if self.peek() == Some('%') => {
                    let token = self.tokenize_template_block()?;
                    self.add_token(token)?;
                }
                Some(c) if c.is_whitespace() => {
                    let tokens = self.tokenize_whitespace()?;
                    for token in tokens {
                        self.add_token(token)?;
                    }
                }
                Some(c) if c.is_alphabetic() || c == '_' => {
                    let token = self.tokenize_identifier_or_keyword()?;
                    self.add_token(token)?;
                }
                Some(c) if c.is_ascii_digit() => {
                    let token = self.tokenize_number()?;
                    self.add_token(token)?;
                }
                Some('"') | Some('\'') => {
                    let token = self.tokenize_string()?;
                    self.add_token(token)?;
                }
                Some('-') if self.peek() == Some('-') => {
                    let token = self.tokenize_comment()?;
                    self.add_token(token)?;
                }
                Some('/') if self.peek() == Some('*') => {
                    let token = self.tokenize_multiline_comment()?;
                    self.add_token(token)?;
                }
                Some(':') if self.peek() == Some(':') => {
                    let token = self.tokenize_operator()?;
                    self.add_token(token)?;
                }
                Some(':') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::Punctuation,
                        value: self.input[start..end].to_string(),
                        start,
                        pair: None,
                    })?;
                }
                Some(c) if "+-*/=<>!@$".contains(c) => {
                    let token = self.tokenize_operator()?;
                    self.add_token(token)?;
                }
                Some(c) if "(),.;".contains(c) => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::Punctuation,
                        value: self.input[start..end].to_string(),
                        start,
                        pair: None,
                    })?;
                }
                _ => {
                    // Unknown token, skip or error
                    self.advance();
                }
            }
        }
        let eof_start = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        self.add_token(Token {
            token_type: TokenType::EOF,
            value: String::new(),
            start: eof_start,
            pair: None,
        })?;
        Ok(TokenizerResult { tokens: self.tokens.clone() })
    }

    fn current_char(&self) -> Option<char> {
        self.current_pos.map(|(_, c)| c)
    }

    fn peek(&self) -> Option<char> {
        self.chars_iter.clone().next().map(|(_, c)| c)
    }

    fn advance(&mut self) {
        self.current_pos = self.chars_iter.next();
    }

    fn tokenize_whitespace(&mut self) -> Result<Vec<Token>, TokenizerError> {
        let mut tokens = Vec::new();
        while let Some(c) = self.current_char() {
            if !c.is_whitespace() {
                break;
            }
            if c == '\n' {
                let start = self.current_pos.map(|(b, _)| b).unwrap();
                tokens.push(Token {
                    token_type: TokenType::Newline,
                    value: "\n".to_string(),
                    start,
                    pair: None,
                });
                self.advance();
            } else {
                // collect consecutive non-newline whitespace
                let ws_start = self.current_pos.map(|(b, _)| b).unwrap();
                while let Some(c) = self.current_char() {
                    if c == '\n' || !c.is_whitespace() {
                        break;
                    }
                    self.advance();
                }
                let ws_end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                if ws_start < ws_end {
                    tokens.push(Token {
                        token_type: TokenType::Whitespace,
                        value: self.input[ws_start..ws_end].to_string(),
                        start: ws_start,
                        pair: None,
                    });
                }
            }
        }
        Ok(tokens)
    }

    fn tokenize_identifier_or_keyword(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        while self
            .current_char()
            .map_or(false, |c| c.is_alphanumeric() || c == '_')
        {
            self.advance();
        }
        let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        let ident = &self.input[start..end];

        // Check if it's a keyword (case-insensitive comparison)
        let ident_upper = ident.to_uppercase();
        if self.keywords.contains_key(ident_upper.as_str()) {
            Ok(Token {
                token_type: TokenType::Keyword,
                value: ident.to_string(),
                start,
                pair: None,
            })
        } else {
            Ok(Token {
                token_type: TokenType::Identifier,
                value: ident.to_string(),
                start,
                pair: None,
            })
        }
    }

    fn tokenize_number(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        while self
            .current_char()
            .map_or(false, |c| c.is_ascii_digit() || c == '.')
        {
            self.advance();
        }
        let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        Ok(Token {
            token_type: TokenType::Number,
            value: self.input[start..end].to_string(),
            start,
            pair: None,
        })
    }

    fn tokenize_string(&mut self) -> Result<Token, TokenizerError> {
        let quote = match self.current_char() {
            Some(q) => q,
            None => return Err(TokenizerError::UnterminatedString { position: self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len()) }),
        };
        self.advance(); // consume opening quote
        let start = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());

        while let Some(c) = self.current_char() {
            if c == quote {
                let end = self.current_pos.map(|(b, _)| b).unwrap();
                self.advance(); // consume closing quote
                return Ok(Token {
                    token_type: TokenType::StringLiteral,
                    value: self.input[start..end].to_string(),
                    start,
                    pair: None,
                });
            }
            self.advance();
        }

        // If we reach here, we didn't find the closing quote
        Err(TokenizerError::UnterminatedString { position: start })
    }

    fn tokenize_operator(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        while self
            .current_char()
            .map_or(false, |c| "+-*/=<>!@$:".contains(c))
        {
            self.advance();
        }
        let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        Ok(Token {
            token_type: TokenType::Operator,
            value: self.input[start..end].to_string(),
            start,
            pair: None,
        })
    }

    fn tokenize_comment(&mut self) -> Result<Token, TokenizerError> {
        self.advance(); // consume first -
        self.advance(); // consume second -
        let start = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        while self.current_char().map_or(false, |c| c != '\n') {
            self.advance();
        }
        let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        Ok(Token {
            token_type: TokenType::Comment,
            value: self.input[start..end].to_string(),
            start,
            pair: None,
        })
    }

    fn tokenize_multiline_comment(&mut self) -> Result<Token, TokenizerError> {
        self.advance(); // consume /
        self.advance(); // consume *
        let start = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());

        loop {
            if self.current_char() == Some('*') && self.peek() == Some('/') {
                let end = self.current_pos.map(|(b, _)| b).unwrap();
                self.advance(); // consume *
                self.advance(); // consume /
                return Ok(Token {
                    token_type: TokenType::Comment,
                    value: self.input[start..end].to_string(),
                    start,
                    pair: None,
                });
            }
            if self.current_pos.is_none() {
                return Err(TokenizerError::UnterminatedComment { position: start });
            }
            self.advance();
        }
    }

    fn tokenize_template_variable(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        self.advance(); // consume first {
        self.advance(); // consume second {

        while let Some(c) = self.current_char() {
            if c == '}' && self.peek() == Some('}') {
                let end = self.current_pos.map(|(b, _)| b + 2).unwrap();
                self.advance(); // consume first }
                self.advance(); // consume second }
                return Ok(Token {
                    token_type: TokenType::TemplateVariable,
                    value: self.input[start..end].to_string(),
                    start,
                    pair: None,
                });
            }
            self.advance();
        }

        // If we reach here, we didn't find the closing }}
        Err(TokenizerError::UnterminatedTemplate {
            template_type: "variable".to_string(),
            position: start,
        })
    }

    fn tokenize_template_block(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        self.advance(); // consume first {
        self.advance(); // consume %

        while let Some(c) = self.current_char() {
            if c == '%' && self.peek() == Some('}') {
                let end = self.current_pos.map(|(b, _)| b + 2).unwrap();
                self.advance(); // consume %
                self.advance(); // consume }
                return Ok(Token {
                    token_type: TokenType::TemplateBlock,
                    value: self.input[start..end].to_string(),
                    start,
                    pair: None,
                });
            }
            self.advance();
        }

        // If we reach here, we didn't find the closing %}
        Err(TokenizerError::UnterminatedTemplate {
            template_type: "block".to_string(),
            position: start,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::fs;

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
            let result = tokenizer.tokenize().expect("Tokenization should succeed in tests");
            let tokens = &result.tokens;

            // Filter out EOF, whitespace, and newline tokens for comparison (unless explicitly expected)
            let actual_tokens: Vec<&Token> = tokens
                .iter()
                .filter(|t| t.token_type != TokenType::EOF && t.token_type != TokenType::Whitespace && t.token_type != TokenType::Newline)
                .collect();

            // Check each expected token
            for (i, expected) in test_case.tokens.iter().enumerate() {
                if i >= actual_tokens.len() {
                    panic!(
                        "Test '{}' failed: Expected {} tokens, got {}",
                        test_case.name,
                        test_case.tokens.len(),
                        actual_tokens.len()
                    );
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
                        TokenType::Newline => "Newline",
                        TokenType::Comment => "Comment",
                        TokenType::TemplateVariable => "TemplateVariable",
                        TokenType::TemplateBlock => "TemplateBlock",
                        TokenType::EOF => "EOF",
                    };

                    if expected_type != actual_type {
                        panic!(
                            "Test '{}' failed at token {}: Expected type '{}', got '{}'",
                            test_case.name, i, expected_type, actual_type
                        );
                    }
                }

                // Check value if specified
                if let Some(ref expected_value) = expected.value {
                    if expected_value != &actual.value {
                        panic!(
                            "Test '{}' failed at token {}: Expected value '{}', got '{}'",
                            test_case.name, i, expected_value, actual.value
                        );
                    }
                }

                // Check position if specified
                if let Some(expected_pos) = expected.position {
                    if expected_pos != actual.start {
                        panic!(
                            "Test '{}' failed at token {}: Expected position {}, got {}",
                            test_case.name, i, expected_pos, actual.start
                        );
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
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");

                println!("\n=== Running tests from {} ===", file_name);
                run_yaml_tests(path.to_str().unwrap());
            }
        }
    }

    #[test]
    fn test_basic_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/basic.yml");
    }

    #[test]
    fn test_ansi_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/ansi.yml");
    }

    #[test]
    fn test_jinja_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/jinja.yml");
    }

    #[test]
    fn test_snowflake_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/snowflake.yml");
    }
}
