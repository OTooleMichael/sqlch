use sql_formatter::tokenizer::{Tokenizer, TokenType};
use sql_formatter::formatter::{Formatter, KeywordCaseRule, IndentRule};
use std::io::{self, Write};

fn main() {
    println!("SQL Formatter Demo");
    println!("==================");
    println!("Enter SQL to format (or 'quit' to exit):");
    println!("Commands:");
    println!("  /upper  - Format with uppercase keywords");
    println!("  /lower  - Format with lowercase keywords");
    println!("  /indent - Format with indentation");
    println!("  /all    - Format with all rules");
    println!();

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

        if input == "quit" {
            break;
        }

        if input.is_empty() {
            continue;
        }

        // Check for commands
        let (sql, formatter) = if input.starts_with("/upper ") {
            (&input[7..], Formatter::new().add_rule(Box::new(KeywordCaseRule::upper())))
        } else if input.starts_with("/lower ") {
            (&input[7..], Formatter::new().add_rule(Box::new(KeywordCaseRule::lower())))
        } else if input.starts_with("/indent ") {
            (&input[8..], Formatter::new().add_rule(Box::new(IndentRule::spaces(2))))
        } else if input.starts_with("/all ") {
            (&input[5..], Formatter::new()
                .add_rule(Box::new(KeywordCaseRule::upper()))
                .add_rule(Box::new(IndentRule::spaces(2))))
        } else {
            // Default: just tokenize
            let mut tokenizer = Tokenizer::new(input);
            let result = match tokenizer.tokenize() {
                Ok(result) => result,
                Err(err) => {
                    println!("Tokenization error: {}", err);
                    continue;
                }
            };
            let tokens = &result.tokens;

            println!("Tokens:");
            for (i, token) in tokens.iter().enumerate() {
                if token.token_type == TokenType::EOF {
                    continue;
                }
                println!("  {:2}: {:15} '{}'", i, format!("{:?}", token.token_type), token.value);
            }
            println!();
            continue;
        };

        // Format the SQL
        let formatted = match formatter.format_sql(sql) {
            Ok(formatted) => formatted,
            Err(err) => {
                println!("Formatting error: {}", err);
                continue;
            }
        };
        println!("Formatted SQL:");
        println!("{}", formatted);
        println!();
    }
}
