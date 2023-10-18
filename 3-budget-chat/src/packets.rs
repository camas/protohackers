#[derive(Debug)]
pub enum ClientPacket {
    Line(String),
}

#[derive(Debug)]
pub enum ServerPacket {
    WelcomeMessage,
    UserJoined { username: String },
    UserLeft { username: String },
    UsersInRoom { usernames: Vec<String> },
    ChatMessage { username: String, message: String },
}
