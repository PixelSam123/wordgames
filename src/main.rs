use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing, Router, Server,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "wordgames=debug,tower_http=debug,axum::rejection=trace".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Initialize word bank on boot
    let word_bank: Vec<DatamuseRes> =
        reqwest::get("http://api.datamuse.com/words?sp=?????&max=500")
            .await?
            .json()
            .await?;

    let state = Arc::new(AppState {
        global_message_tx: broadcast::channel(64).0,
        players: Mutex::new(HashSet::new()),
        word_bank,
        game_state: None,
    });

    let app = Router::new()
        .route("/ws/anagram", routing::get(ws_anagram_handler))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::debug!("Listening on {}", addr);
    Server::bind(&addr).serve(app.into_make_service()).await?;

    Ok(())
}

struct AppState {
    global_message_tx: broadcast::Sender<String>,
    players: Mutex<HashSet<String>>,
    word_bank: Vec<DatamuseRes>,
    game_state: Option<GameState>,
}

#[derive(Deserialize)]
struct DatamuseRes {
    word: String,
    score: i32,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "content")]
enum ServerMessage {
    ChatMessage(String),
    OngoingGameInfo {
        word_to_guess: String,
        round_finish_time: String,
    },
}

struct GameState {
    round_status: RoundStatus,
    player_to_points: HashMap<String, i32>,
}

enum RoundStatus {
    Guessing(String),
    RevealingAnswer(String),
}

async fn ws_anagram_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| ws_anagram(socket, state))
}

async fn ws_anagram(ws_stream: WebSocket, state: Arc<AppState>) {
    tracing::debug!("A player entered! Waiting for username entry.");
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // Handle username registration
    let name = loop {
        if let Some(Ok(Message::Text(name))) = ws_rx.next().await {
            let is_username_taken = {
                let mut players = state.players.lock().unwrap();
                let is_username_taken = players.contains(&name);

                if !is_username_taken {
                    players.insert(name.clone());
                }

                is_username_taken
            };

            if !is_username_taken {
                break name;
            }

            ws_tx
                .send(Message::Text("Username already taken!".to_owned()))
                .await
                .unwrap();
        } else {
            tracing::debug!("A player left before entering their username!");
            return;
        }
    };

    // Now handle the messages.
    let mut global_message_rx = state.global_message_tx.subscribe();

    tracing::debug!("{} joined!", &name);
    state
        .global_message_tx
        .send(format!("{} joined!", &name))
        .unwrap();

    let mut send_task = tokio::spawn(async move {
        while let Ok(message) = global_message_rx.recv().await {
            if ws_tx.send(Message::Text(message)).await.is_err() {
                break;
            }
        }
    });

    let global_message_tx = state.global_message_tx.clone();
    let cloned_name = name.clone();
    let cloned_state = state.clone();

    let mut recv_task = tokio::spawn(async move {
        handle_ws_recv(cloned_state, global_message_tx, cloned_name, &mut ws_rx).await;
    });

    // Handle exiting
    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    }

    tracing::debug!("{} left!", &name);
    state
        .global_message_tx
        .send(format!("{} left!", &name))
        .unwrap();
    state.players.lock().unwrap().remove(&name);
}

async fn handle_ws_recv(
    app_state: Arc<AppState>,
    global_message_tx: broadcast::Sender<String>,
    name: String,
    ws_rx: &mut SplitStream<WebSocket>,
) {
    while let Some(Ok(Message::Text(message))) = ws_rx.next().await {
        if message.starts_with("/start") {
            let splitted: Vec<&str> = message.split(' ').skip(1).take(1).collect();

            if splitted.len() == 1 {
                splitted[0].parse::<i32>().map_or_else(
                    |_| {
                        global_message_tx
                            .send(format!(
                                "To {name}: Invalid start match format! Not a number."
                            ))
                            .unwrap();
                    },
                    |timer_duration| {
                        if app_state.game_state.is_none() {
                            global_message_tx
                                .send(format!(
                                    "To {name}: Can't start match! One's already ongoing."
                                ))
                                .unwrap();
                        } else {
                            global_message_tx
                                .send(format!(
                                    "New Game with round timer {timer_duration} started!"
                                ))
                                .unwrap();
                        }
                    },
                );
            } else {
                global_message_tx
                    .send(format!("To {name}: Invalid start match format!"))
                    .unwrap();
            }
        } else {
            global_message_tx
                .send(format!("{name}: {message}"))
                .unwrap();
        }
    }
}
