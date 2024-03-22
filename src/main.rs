use std::{env, sync::Arc};

use paho_mqtt as mqtt;
use tokio::{
    sync::RwLock,
    time::{sleep, Duration},
};
use tokio_seqpacket::UnixSeqpacket;
use tokio_stream::StreamExt;

const TOPIC_COMMAND: &str = "agent/command";
const TOPIC_RESPONSE: &str = "agent/response";

async fn mqtt_reconnect(client: &mqtt::AsyncClient) {
    loop {
        if client.reconnect().await.is_ok() {
            mqtt_subscribe(client).await;
            println!("Successfully reconnected");
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn mqtt_subscribe(client: &mqtt::AsyncClient) {
    let subscribe_result = client.subscribe(TOPIC_COMMAND, 0).await.and_then(|rsp| {
        rsp.subscribe_response()
            .ok_or(mqtt::Error::General("Bad response"))
    });
    if let Err(err) = subscribe_result {
        let _ = client.disconnect(None).await;
        panic!("Error subscribing to topics: {:?}", err);
    }
}

async fn mqtt_start_consuming(
    mut mqtt_client: mqtt::AsyncClient,
    agent_socket: Arc<RwLock<UnixSeqpacket>>,
) {
    let mut stream = mqtt_client.get_stream(25);

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(true)
        .finalize();

    println!(
        "Connecting to the MQTT broker at '{}'...",
        mqtt_client.server_uri()
    );
    // Make the connection to the broker
    match mqtt_client.connect(conn_opts).await {
        Ok(response) => {
            if let Some(response) = response.connect_response() {
                println!(
                    "Connected to: '{}' with MQTT version {}",
                    response.server_uri, response.mqtt_version
                );

                mqtt_subscribe(&mqtt_client).await;
            }
        }
        Err(e) => {
            panic!("Error connecting to the MQTT broker: {:?}", e);
        }
    }

    while let Some(msg) = stream.next().await {
        match msg {
            Some(msg) => {
                if msg.topic() == TOPIC_COMMAND {
                    let payload = msg.payload_str();
                    let _ = agent_socket.read().await.send(payload.as_bytes()).await;
                }
            }
            None => {
                println!("MQTT Connection lost. Reconnecting...");
                mqtt_reconnect(&mqtt_client).await;
            }
        }
    }
}

async fn agent_socket_connect(agent_socket_path: &str) -> UnixSeqpacket {
    println!(
        "Connecting to the agent socket at '{}'...",
        agent_socket_path
    );
    loop {
        if let Ok(agent_socket) = UnixSeqpacket::connect(agent_socket_path).await {
            println!("Successfully connected to agent socket");
            for msg in [
                r#"{"address":256,"method":"bind"}"#,
                r#"{"key":"auto.report","method":"register"}"#,
                r#"{"key":"auto.forward","method":"register"}"#,
                r#"{"key":"lanbox.event","method":"register"}"#,
            ] {
                let _ = agent_socket.send(msg.as_bytes()).await;
            }
            return agent_socket;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn agent_socket_start_consuming(
    agent_socket: Arc<RwLock<UnixSeqpacket>>,
    agent_socket_path: &str,
    mqtt_client: mqtt::AsyncClient,
) {
    let mut buf = [0; 4096];
    loop {
        let n = agent_socket.read().await.recv(&mut buf).await.unwrap();

        if n == 0 {
            println!("Error reading from agent socket. Try reconnecting...");
            *(agent_socket.write().await) = agent_socket_connect(&agent_socket_path).await;
            continue;
        }

        let _ = mqtt_client
            .publish(mqtt::Message::new(TOPIC_RESPONSE, &buf[..n], 0))
            .await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mqtt_ip = env::args().nth(1);
    let mqtt_host = match mqtt_ip {
        Some(ip) => format!("mqtt://{}:1883", ip),
        None => "mqtt://localhost:1883".to_string(),
    };
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(mqtt_host)
        .client_id("agent2mqtt")
        .finalize();
    let mqtt_client = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        panic!("Error creating the MQTT client: {:?}", e);
    });

    let agent_socket_path = env::args()
        .nth(2)
        .unwrap_or("/tmp/miio_agent.socket".to_string());
    let agent_socket = Arc::new(RwLock::new(agent_socket_connect(&agent_socket_path).await));

    tokio::spawn(mqtt_start_consuming(
        mqtt_client.clone(),
        agent_socket.clone(),
    ));

    agent_socket_start_consuming(agent_socket, &agent_socket_path, mqtt_client).await;
}
