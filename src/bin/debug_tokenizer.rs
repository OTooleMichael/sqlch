use sql_formatter::tokenizer::{TokenType, Tokenizer};

fn main() {
    let sql = "SELECT * -- comment\nFROM users";
    println!("Tokenizing: {sql:?}");
    println!("Characters:");
    for (i, c) in sql.char_indices() {
        println!("  {i}: {c:?} ({})", c as u32);
    }
    println!();

    let mut tokenizer = Tokenizer::new(sql);
    let result = tokenizer.tokenize().expect("Should tokenize");

    println!("Tokens produced:");
    for (i, token) in result.tokens.iter().enumerate() {
        if token.token_type != TokenType::EOF {
            println!(
                "  {i}: {:?} - value: {:?} - start: {}",
                token.token_type, token.value, token.start
            );
            if !token.comments.is_empty() {
                println!("     Comments: {:?}", token.comments);
            }
        }
    }
}
