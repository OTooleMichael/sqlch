use crate::tokenizer::{Token, TokenType};

/// Trait for formatting rules that operate on token streams
pub trait Rule {
    /// Apply the rule to a token stream
    fn apply(&self, tokens: &[Token]) -> Vec<Token>;

    /// Get the name of this rule
    fn name(&self) -> &str;
}

/// Configuration for keyword case formatting
#[derive(Debug, Clone)]
pub enum KeywordCase {
    Upper,
    Lower,
    Preserve,
}

/// Configuration for indentation
#[derive(Debug, Clone)]
pub enum IndentType {
    Spaces,
    Tabs,
}

/// Indentation rule for managing consistent indentation
#[derive(Debug)]
pub struct IndentRule {
    indent_type: IndentType,
    indent_size: usize,
}

/// Edit operations for stateful formatting
#[derive(Debug, Clone)]
enum Edit {
    Insert(usize, Token),
    Replace(usize, Token),
    Delete(usize),
}

/// Keyword case formatting rule
#[derive(Debug)]
pub struct KeywordCaseRule {
    case: KeywordCase,
}

impl KeywordCaseRule {
    pub fn new(case: KeywordCase) -> Self {
        Self { case }
    }

    pub fn upper() -> Self {
        Self::new(KeywordCase::Upper)
    }

    pub fn lower() -> Self {
        Self::new(KeywordCase::Lower)
    }

    pub fn preserve() -> Self {
        Self::new(KeywordCase::Preserve)
    }
}

impl Rule for KeywordCaseRule {
    fn apply(&self, tokens: &[Token]) -> Vec<Token> {
        tokens
            .iter()
            .map(|token| {
                if token.token_type == TokenType::Keyword {
                    let new_value = match self.case {
                        KeywordCase::Upper => token.value.to_uppercase(),
                        KeywordCase::Lower => token.value.to_lowercase(),
                        KeywordCase::Preserve => token.value.clone(),
                    };

                    Token {
                        token_type: token.token_type.clone(),
                        value: new_value,
                        start: token.start,
                        pair: None,
                    }
                } else {
                    token.clone()
                }
                })
            .collect()
    }

    fn name(&self) -> &str {
        "keyword_case"
    }
}

impl IndentRule {
    fn should_add_newline_before_keyword(
        &self,
        keyword: &str,
        next_tokens: &[Token],
        prev_tokens: &[Token],
    ) -> bool {
        match keyword {
            "SELECT" => {
                // Don't add newline if next is DISTINCT, ALL, TOP, etc.
                !self.is_modifier_keyword(next_tokens)
        }
            "INSERT" => {
                // Don't add newline if next is INTO
                !self.next_is_keyword(next_tokens, "INTO")
        }
            "UPDATE" => {
                // Don't add newline if next is SET
                !self.next_is_keyword(next_tokens, "SET")
        }
            "INTO" => {
                // Don't add newline if previous was INSERT
                !self.prev_is_keyword(prev_tokens, "INSERT")
        }
            "SET" => {
                // Don't add newline if previous was UPDATE
                !self.prev_is_keyword(prev_tokens, "UPDATE")
        }
            "FROM" | "WHERE" | "HAVING" | "ORDER" | "GROUP" | "LIMIT" | "DELETE" => true,
            _ => false,
        }
    }

    fn prev_is_keyword(&self, tokens: &[Token], expected: &str) -> bool {
        for token in tokens.iter().rev() {
            match token.token_type {
                TokenType::Whitespace => continue,
                TokenType::Keyword => {
                    return token.value.to_uppercase() == expected;
                }
                _ => return false,
        }
        }
        false
    }

    fn is_modifier_keyword(&self, tokens: &[Token]) -> bool {
        for token in tokens {
            match token.token_type {
                TokenType::Whitespace => continue,
                TokenType::Keyword => {
                    return matches!(
                        token.value.to_uppercase().as_str(),
                        "DISTINCT" | "ALL" | "TOP"
                    );
                }
                _ => return false,
        }
        }
        false
    }

    fn next_is_keyword(&self, tokens: &[Token], expected: &str) -> bool {
        for token in tokens {
            match token.token_type {
                TokenType::Whitespace => continue,
                TokenType::Keyword => {
                    return token.value.to_uppercase() == expected;
                }
                _ => return false,
        }
        }
        false
    }
}

/// Main formatter that applies rules to token streams
pub struct Formatter {
    rules: Vec<Box<dyn Rule>>,
}

/// Apply a list of edits to a token stream
fn apply_edits(tokens: &[Token], edits: &[Edit]) -> Vec<Token> {
    let mut result = tokens.to_vec();
    // Sort edits by position in descending order to maintain indices
    let mut sorted_edits = edits.to_vec();
    sorted_edits.sort_by(|a, b| {
        let pos_a = match a {
            Edit::Insert(p, _) => *p,
            Edit::Replace(p, _) => *p,
            Edit::Delete(p) => *p,
        };
        let pos_b = match b {
            Edit::Insert(p, _) => *p,
            Edit::Replace(p, _) => *p,
            Edit::Delete(p) => *p,
        };
        pos_b.cmp(&pos_a)
    });
    for edit in sorted_edits {
        match edit {
            Edit::Insert(pos, token) => result.insert(pos, token.clone()),
            Edit::Replace(pos, token) => result[pos] = token.clone(),
            Edit::Delete(pos) => {
                result.remove(pos);
        }
        }
    }
    result
}

impl Formatter {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a rule to the formatter
    pub fn add_rule(mut self, rule: Box<dyn Rule>) -> Self {
        self.rules.push(rule);
        self
    }

    /// Format a token stream by applying all rules
    pub fn format(&self, tokens: &[Token]) -> Vec<Token> {
        let mut result = tokens.to_vec();

        for rule in &self.rules {
            result = rule.apply(&result);
        }

        result
    }

    /// Format a SQL string by tokenizing and then formatting
    pub fn format_sql(&self, sql: &str) -> Result<String, crate::tokenizer::TokenizerError> {
        let mut tokenizer = crate::tokenizer::Tokenizer::new(sql);
        let result = tokenizer.tokenize()?;
        let formatted_tokens = self.format(&result.tokens);

        // Convert tokens back to string with quotes for strings
        let mut output = String::new();
        for token in formatted_tokens {
            if token.token_type != TokenType::EOF {
                let value = match token.token_type {
                    TokenType::StringLiteral => format!("'{}'", token.value),
                    TokenType::Newline => "\n".to_string(),
                    _ => token.value,
                };
                output.push_str(&value);
        }
        }

        Ok(output.trim().to_string())
    }
}

impl Default for Formatter {
    fn default() -> Self {
        Self::new()
    }
    }

#[cfg(test)]

mod tests {
    use super::*;

    

    #[test]
    fn test_formatter_rules_basic() {
        run_format_tests("test/fixtures/formatter/basic.yml");
    }
    #[test]
    fn test_formatter_rules_complex() {
        run_format_tests("test/fixtures/formatter/complex.yml");
    }
}
    use super::*;
    use crate::tokenizer::TokenType;
    use serde::Deserialize;
    use std::fs;

    #[derive(Debug, Deserialize)]
    struct FormatTestCase {
        name: String,
        input: String,
        #[serde(default)]
        rules: Option<Vec<TestRule>>,
        #[serde(default)]
        settings: Option<TestSettings>,
        expected: String,
    }

    #[derive(Debug, Deserialize)]
    struct TestSettings {
        keyword_case: Option<String>,
        indent_style: Option<String>,
        indent_size: Option<usize>,
    }

    #[derive(Debug, Deserialize)]
    struct TestRule {
        name: String,
        settings: serde_yaml::Value,
    }

    #[derive(Debug, Deserialize)]
    struct FormatTestFile {
        file: Vec<FormatTestCase>,
}

pub struct Formatter {
    rules: Vec<Box<dyn Rule>>,
}

impl Formatter {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
        }
    }

    pub fn add_rule(mut self, rule: Box<dyn Rule>) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn format(&self, input: &str) -> String {
        // Placeholder implementation
        input.to_string()
    }
}

impl Default for Formatter {
    fn default() -> Self {
        Self::new()
    }
}

fn load_format_test_file(path: &str) -> FormatTestFile {
        let content = fs::read_to_string(path).expect("Failed to read test file");
        serde_yaml::from_str(&content).expect("Failed to parse YAML")
    }

    fn create_rule_from_test(test_rule: &TestRule) -> Box<dyn Rule> {
        match test_rule.name.as_str() {
            "keyword_case" => {
                if let Some(case) = test_rule.settings.get("case") {
                    match case.as_str().unwrap() {
                        "upper" => Box::new(KeywordCaseRule::upper()),
                        "lower" => Box::new(KeywordCaseRule::lower()),
                        "preserve" => Box::new(KeywordCaseRule::preserve()),
                        _ => panic!("Unknown keyword case: {:?}", case),
                    }
                } else {
                    Box::new(KeywordCaseRule::upper())
                }
        }
            "indent" => {
                if let Some(indent_type) = test_rule.settings.get("type") {
                    match indent_type.as_str().unwrap() {
                        "spaces" => {
                            let size = test_rule
                                .settings
                                .get("size")
                                .and_then(|s| s.as_u64())
                                .unwrap_or(2) as usize;
                            Box::new(IndentRule::spaces(size))
                        }
                        "tabs" => Box::new(IndentRule::tabs()),
                        _ => panic!("Unknown indent type: {:?}", indent_type),
                    }
                } else {
                    Box::new(IndentRule::spaces(2))
                }
        }
            _ => panic!("Unknown rule: {}", test_rule.name),
        }
    }

    fn run_format_tests(test_file_path: &str) {
        let test_file = load_format_test_file(test_file_path);

        for test_case in test_file.file {
            println!("Running format test: {}", test_case.name);

            // Tokenize input
            let mut tokenizer = crate::tokenizer::Tokenizer::new(&test_case.input);
            let input_result = match tokenizer.tokenize() {
                Ok(result) => result,
                Err(e) => panic!("Tokenization error: {}", e),
        };
            let input_tokens = &input_result.tokens;

            // Create formatter with rules or settings
            let mut formatter = Formatter::new();
            if let Some(rules) = &test_case.rules {
                for test_rule in rules {
                    let rule = create_rule_from_test(test_rule);
                    formatter = formatter.add_rule(rule);
                }
        }
            if let Some(settings) = &test_case.settings {
                if let Some(kc) = &settings.keyword_case {
                    let rule: Box<dyn Rule> = match kc.as_str() {
                        "upper" => Box::new(KeywordCaseRule::upper()),
                        "lower" => Box::new(KeywordCaseRule::lower()),
                        "preserve" => Box::new(KeywordCaseRule::preserve()),
                        _ => panic!("Unknown keyword_case: {}", kc),
                    };
                    formatter = formatter.add_rule(rule);
                }
                let indent_needed =
                    settings.indent_style.is_some() || settings.indent_size.is_some();
                if indent_needed {
                    let indent_type = match settings.indent_style.as_deref() {
                        Some("tabs") => IndentType::Tabs,
                        _ => IndentType::Spaces,
                    };
                    let indent_size = settings.indent_size.unwrap_or(2);
                    let rule = Box::new(IndentRule::new(indent_type, indent_size));
                    formatter = formatter.add_rule(rule);
                }
        }

            // Apply formatting
            let output_tokens = formatter.format(input_tokens);

            // Convert to string with quotes for strings
            let mut actual_output = String::new();
            for token in output_tokens {
                if token.token_type != TokenType::EOF {
                    let value = match token.token_type {
                        TokenType::StringLiteral => format!("'{}'", token.value),
                        _ => token.value,
                    };
            actual_output.push_str(&value);
                }
            }

            // Normalize whitespace for comparison (trim and normalize newlines)
            let expected_normalized = test_case.expected.trim().replace("\r\n", "\n");
            let actual_normalized = actual_output.trim().replace("\r\n", "\n");

            // Trim trailing spaces from each line
            let expected_lines: Vec<String> = expected_normalized
                .split('\n')
                .map(|line| line.trim_end().to_string())
                .collect();
            let actual_lines: Vec<String> = actual_normalized
                .split('\n')
                .map(|line| line.trim_end().to_string())
                .collect();
            let expected_final = expected_lines.join("\n");
            let actual_final = actual_lines.join("\n");

            if actual_final != expected_final {
                panic!(
                    "Test '{}' failed:\nExpected:\n{}\nGot:\n{}",
                    test_case.name, expected_final, actual_final
                );
            }

            println!("  ✓ Passed");
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_formatter_rules_basic() {
        run_format_tests("test/fixtures/formatter/basic.yml");
    }
    #[test]
    fn test_formatter_rules_complex() {
        run_format_tests("test/fixtures/formatter/complex.yml");
    }
}

