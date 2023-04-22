use std::str;
use tokio::{net::TcpListener, io::{AsyncReadExt, AsyncWriteExt}};

enum Command {
    Invalid,
    Ping { argument: Option<String> }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            loop {
                match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from the socket; err = {:?}", e);
                        return;
                    }
                };

                let arr = parse_input(buf);

                let mut commands: Vec<Command> = vec![];

                for el in arr.elements {
                    match el {
                        Resp::BulkString { num_bytes: _, content } => {
                            let mut split = content.split_whitespace();
                            let cmd = split.nth(0);

                            match cmd {
                                Some("PING") => {
                                    let rest = split.collect::<Vec<&str>>().join(" ");
                                    // Trimming away NUL termination characters
                                    let rest = rest.trim_matches(char::from(0));

                                    if rest.is_empty() {
                                        commands.push(Command::Ping { argument: None })
                                    } else {
                                        commands.push(Command::Ping { argument: Some(String::from(rest)) })
                                    }
                                },
                                _ => commands.push(Command::Invalid),
                            };
                        }
                    };
                }

                for command in commands {
                    let response = match command {
                        Command::Invalid => String::from("INVALID COMMAND"),
                        Command::Ping { argument: None } => String::from("PONG"),
                        Command::Ping { argument: Some(phrase) } => phrase,
                    };

                    println!("Response is {}", response);

                    // Write the data back
                    if let Err(e) = socket.write_all(write_simple_string(&response).as_bytes()).await {
                        eprintln!("failed to write to socket; err = {:?}", e);
                        return;
                    }
                }

            }
        });
    }
}

enum Resp {
    BulkString {
        num_bytes: i32,
        content: String
    }
}

struct RespArray {
    count: i32,
    elements: Vec<Resp>
}

/*
 * buf is
  *1
  $4
  PING
 */
fn parse_input(buf: [u8; 1024]) -> RespArray {
    return parse_resp_array(&buf);
}

fn parse_resp_array(buf: &[u8; 1024]) -> RespArray {
    let buf = str::from_utf8(buf).unwrap().trim_matches(char::from(0)).trim();
    let mut buf = buf.split("\r\n");

    // Parse *n, count = n
    let count = i32::from_str_radix(&buf.nth(0).or(Some("*0")).unwrap()[1..], 10).unwrap();
    let mut elements: Vec<Resp> = vec![];
    let mut buf = buf.collect();

    // Parse each element
    for _ in [..count] {
        elements.push(parse_resp_bulk_string(&mut buf));
    }

    return RespArray { count, elements }
}

// If bulk string and >0, parse 2 lines (num_bytes and content)
fn parse_resp_bulk_string(buf: &mut Vec<&str>) -> Resp {
    let num_bytes = &buf.get(0).unwrap()[1..];
    let num_bytes = i32::from_str_radix(num_bytes, 10).unwrap();

    if num_bytes == 0 {
        return Resp::BulkString { num_bytes: 0, content: String::from("") }
    } else if num_bytes < 0 {
        return Resp::BulkString { num_bytes: -1, content: String::from("") }
    }

    let capacity = usize::try_from(num_bytes).unwrap();
    let content = buf.get(1).unwrap().as_bytes().get(0..capacity);

    match content {
        Some(body) => Resp::BulkString { num_bytes, content: String::from_utf8(body.to_vec()).unwrap() },
        None => Resp::BulkString { num_bytes, content: String::from("") }
    }
}

fn write_simple_string(response: &str) -> String {
    return format!("+{}\r\n", response);
}
