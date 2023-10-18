use std::{collections::HashMap, net::ToSocketAddrs};

use anyhow::anyhow;
use log::{error, info, trace};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpSocket, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use crate::{
    message::{Message, PopulationTarget, SitePopulation},
    model::{PolicyID, SiteID, SpeciesAction},
    server::ServerHandle,
    utilities::{read_message, write_message, PROTOCOL, PROTOCOL_VERSION},
};

static AUTHORITY_ADDR: &str = "pestcontrol.protohackers.com:20547";

pub struct AuthorityClient {
    site_id: SiteID,
    command_tx: mpsc::Sender<Command>,
    population_targets: HashMap<String, MinMax>,
    message_request_tx: mpsc::Sender<MessageRequest>,
    species_with_policy: HashMap<String, (PolicyID, SpeciesAction)>,
}

#[derive(Clone)]
pub struct AuthorityClientHandle {
    command_tx: mpsc::Sender<Command>,
}

enum Command {
    SiteVisit { populations: Vec<SitePopulation> },
}

#[derive(Debug, Clone, Copy)]
struct MinMax {
    min: u32,
    max: u32,
}

enum MessageRequest {
    CreatePolicy {
        species: String,
        action: SpeciesAction,
        policy_id_tx: oneshot::Sender<PolicyID>,
    },
    DeletePolicy {
        policy_id: PolicyID,
    },
}

impl AuthorityClient {
    pub fn run(
        site_id: SiteID,
        server: ServerHandle,
        server_cancellation_token: CancellationToken,
    ) -> AuthorityClientHandle {
        trace!("Running handler for authority site {}", site_id.0);

        let (command_tx, command_rx) = mpsc::channel(1000);
        let (message_request_tx, message_request_rx) = mpsc::channel(1);
        let mut authority_client = AuthorityClient {
            site_id,
            command_tx,
            population_targets: HashMap::new(),
            message_request_tx,
            species_with_policy: HashMap::new(),
        };

        let handle = authority_client.handle();
        let handle_ = handle.clone();
        tokio::spawn(async move {
            let Ok(tcp_socket) = TcpSocket::new_v4() else {
                server_cancellation_token.cancel();
                panic!("Couldn't create socket");
            };
            let Ok(tcp_stream) = tcp_socket
                .connect(AUTHORITY_ADDR.to_socket_addrs().unwrap().next().unwrap())
                .await
            else {
                server_cancellation_token.cancel();
                panic!("Couldn't connect to authority");
            };

            let (setup_targets_tx, setup_targets_rx) = oneshot::channel::<Vec<PopulationTarget>>();

            let stream_task = tokio::spawn(stream_loop(
                tcp_stream,
                site_id,
                setup_targets_tx,
                message_request_rx,
            ));

            let Ok(population_targets) = setup_targets_rx.await else {
                if stream_task.is_finished() {
                    let error = stream_task.await.unwrap().unwrap_err();
                    error!("Failed to setup population targets: {error}");
                } else {
                    error!("Failed to setup population targets");
                }
                return;
            };
            for population_target in population_targets {
                authority_client.population_targets.insert(
                    population_target.species,
                    MinMax {
                        min: population_target.min,
                        max: population_target.max,
                    },
                );
            }

            let command_task = tokio::spawn(command_loop(authority_client, command_rx));

            tokio::select! {
                result = stream_task => {
                    error!("Stream task failed: {}", result.unwrap_err());
                }
                result = command_task => {
                    error!("Command task failed: {}", result.unwrap_err());
                }
                _ = server.wait_shutdown() => {}
            };
            server.shutdown();
        });

        handle_
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::SiteVisit { populations } => self.record_site_visit(populations).await,
        }
    }

    async fn record_site_visit(&mut self, populations: Vec<SitePopulation>) {
        info!("Site {} site visit: {populations:?}", self.site_id.0);

        let populations: HashMap<String, u32> = populations
            .into_iter()
            .map(|population| (population.species, population.count))
            .collect();
        for (species, target) in self.population_targets.iter() {
            let population_count = match populations.get(species) {
                Some(v) => *v as u64,
                None => 0,
            };

            let action;
            if population_count < target.min as u64 {
                action = Some(SpeciesAction::Conserve);
            } else if population_count > target.max as u64 {
                action = Some(SpeciesAction::Cull);
            } else {
                action = None;
            }

            let current_policy = self.species_with_policy.remove(species);

            match (action, current_policy) {
                (None, None) => {
                    trace!(
                        "Site {} No action for {} and no current policy",
                        self.site_id.0,
                        species
                    );
                }
                (Some(action), None) => {
                    trace!(
                        "Site {} Creating action {action:?} for {} and no current policy",
                        self.site_id.0,
                        species
                    );
                    let policy_id = self.create_policy(species.clone(), action).await;
                    self.species_with_policy
                        .insert(species.clone(), (policy_id, action));
                }
                (None, Some((policy_id, _))) => {
                    trace!(
                        "Site {} No action for {} and deleting current policy",
                        self.site_id.0,
                        species
                    );
                    self.delete_policy(policy_id).await;
                }
                (Some(new_action), Some((old_policy_id, old_action))) => {
                    if new_action == old_action {
                        trace!(
                            "Site {} Action {new_action:?} for {} is same as current policy",
                            self.site_id.0,
                            species
                        );
                        self.species_with_policy
                            .insert(species.clone(), (old_policy_id, old_action));
                        continue;
                    }
                    trace!(
                        "Site {} Creating action {new_action:?} for {} and deleting current policy",
                        self.site_id.0,
                        species
                    );
                    self.delete_policy(old_policy_id).await;
                    let policy_id = self.create_policy(species.clone(), new_action).await;
                    self.species_with_policy
                        .insert(species.clone(), (policy_id, new_action));
                }
            }
        }
    }

    async fn create_policy(&self, species: String, action: SpeciesAction) -> PolicyID {
        let (policy_id_tx, policy_id_rx) = oneshot::channel();
        let _ = self
            .message_request_tx
            .send(MessageRequest::CreatePolicy {
                species,
                action,
                policy_id_tx,
            })
            .await;
        policy_id_rx.await.unwrap()
    }

    async fn delete_policy(&self, policy_id: PolicyID) {
        let _ = self
            .message_request_tx
            .send(MessageRequest::DeletePolicy { policy_id })
            .await;
    }

    fn handle(&self) -> AuthorityClientHandle {
        AuthorityClientHandle {
            command_tx: self.command_tx.clone(),
        }
    }
}

async fn command_loop(
    mut authority_client: AuthorityClient,
    mut command_rx: mpsc::Receiver<Command>,
) -> anyhow::Result<()> {
    loop {
        let Some(command) = command_rx.recv().await else {
            return Err(anyhow!("Couldn't receive command"));
        };
        authority_client.handle_command(command).await;
    }
}

async fn stream_loop(
    mut tcp_stream: TcpStream,
    site_id: SiteID,
    setup_targets_tx: oneshot::Sender<Vec<PopulationTarget>>,
    mut message_request_rx: mpsc::Receiver<MessageRequest>,
) -> anyhow::Result<()> {
    let mut data = Vec::new();

    macro_rules! exit {
        ($error:expr) => {{
            let _ = tcp_stream
                .write_all(
                    &Message::Error {
                        message: $error.to_string(),
                    }
                    .to_bytes(),
                )
                .await;
            return Err(anyhow!($error));
        }};
    }

    let hello_message = Message::Hello {
        protocol: PROTOCOL.to_string(),
        version: PROTOCOL_VERSION,
    };
    write_message(&mut tcp_stream, hello_message).await?;

    let message = read_message(&mut tcp_stream, &mut data).await;
    match message {
        Ok(Message::Hello { .. }) => {}
        Ok(message) => {
            error!("Invalid first message sent: {message:?}");
            exit!("Invalid first message");
        }
        Err(e) => {
            error!("Error while reading message: {e}");
            exit!(e);
        }
    }

    let dial_message = Message::DialAuthority { site_id };
    write_message(&mut tcp_stream, dial_message).await?;

    let message = read_message(&mut tcp_stream, &mut data).await;
    let population_targets = match message {
        Ok(Message::TargetPopulations {
            site_id: given_site_id,
            populations,
        }) => {
            assert_eq!(site_id, given_site_id);
            populations
        }
        Ok(message) => {
            error!("Invalid second message sent: {message:?}");
            exit!("Invalid second message");
        }
        Err(e) => {
            error!("Error while reading message: {e}");
            exit!(e);
        }
    };
    let _ = setup_targets_tx.send(population_targets);

    loop {
        let Some(message_request) = message_request_rx.recv().await else {
            return Ok(());
        };

        match message_request {
            MessageRequest::CreatePolicy {
                species,
                action,
                policy_id_tx,
            } => {
                let create_policy_message = Message::CreatePolicy { species, action };
                write_message(&mut tcp_stream, create_policy_message).await?;

                let message = read_message(&mut tcp_stream, &mut data).await;
                let policy_id = match message {
                    Ok(Message::PolicyResult { policy_id }) => policy_id,
                    Ok(message) => {
                        error!("Invalid CreatePolicy response message sent: {message:?}");
                        exit!("Invalid CreatePolicy response message");
                    }
                    Err(e) => {
                        error!("Error while reading message: {e}");
                        exit!(e);
                    }
                };
                let _ = policy_id_tx.send(policy_id);
            }
            MessageRequest::DeletePolicy { policy_id } => {
                let delete_policy_message = Message::DeletePolicy { policy_id };
                write_message(&mut tcp_stream, delete_policy_message).await?;

                let message = read_message(&mut tcp_stream, &mut data).await;
                match message {
                    Ok(Message::Ok) => {}
                    Ok(message) => {
                        error!("Invalid CreatePolicy response message sent: {message:?}");
                        exit!("Invalid CreatePolicy response message");
                    }
                    Err(e) => {
                        error!("Error while reading message: {e}");
                        exit!(e);
                    }
                };
            }
        }
    }
}

impl AuthorityClientHandle {
    pub async fn site_visit(&self, populations: Vec<SitePopulation>) {
        let _ = self
            .command_tx
            .send(Command::SiteVisit { populations })
            .await;
    }
}
