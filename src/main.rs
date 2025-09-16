use sql_formatter::tokenizer::{TokenType, Tokenizer};
use std::io::{self, Write};

fn main() {
    println!("SQL Tokenizer Demo");
    println!("==================");
    println!("Enter SQL to tokenize (or 'quit' to exit):");
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

        // Tokenize the input
        let mut tokenizer = Tokenizer::new(input);
        let result = match tokenizer.tokenize() {
            Ok(result) => result,
            Err(err) => {
                println!("Tokenization error: {err}");
                continue;
            }
        };

        println!("Hash: {}", result.hash);
        println!("Tokens:");
        for (i, token) in result.tokens.iter().enumerate() {
            if token.token_type == TokenType::EOF {
                continue;
            }
            println!(
                "  {:2}: {:15} '{}'",
                i,
                format!("{:?}", token.token_type),
                token.value()
            );
        }
        println!();
    }
}
