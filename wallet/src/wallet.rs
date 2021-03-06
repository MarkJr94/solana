use bincode::serialize;
use bs58;
use chrono::prelude::*;
use clap::ArgMatches;
use log::*;
use serde_json;
use serde_json::json;
use solana::rpc_service::RPC_PORT;
use solana::rpc_status::RpcSignatureStatus;
use solana_budget_api;
use solana_budget_api::budget_transaction::BudgetTransaction;
#[cfg(test)]
use solana_client::rpc_mock::{request_airdrop_transaction, MockRpcClient as RpcClient};
#[cfg(not(test))]
use solana_client::rpc_request::RpcClient;
use solana_client::rpc_request::{get_rpc_request_str, RpcRequest};
#[cfg(not(test))]
use solana_drone::drone::request_airdrop_transaction;
use solana_drone::drone::DRONE_PORT;
use solana_sdk::bpf_loader;
use solana_sdk::hash::Hash;
use solana_sdk::loader_transaction::LoaderTransaction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, KeypairUtil, Signature};
use solana_sdk::system_transaction::SystemTransaction;
use solana_sdk::timing::{DEFAULT_TICKS_PER_SLOT, NUM_TICKS_PER_SECOND};
use solana_sdk::transaction::Transaction;
use solana_vote_api::vote_instruction::VoteInstruction;
use solana_vote_api::vote_transaction::VoteTransaction;
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;
use std::{error, fmt, mem};

const USERDATA_CHUNK_SIZE: usize = 256;

#[derive(Debug, PartialEq)]
pub enum WalletCommand {
    Address,
    Airdrop(u64),
    Balance,
    Cancel(Pubkey),
    Confirm(Signature),
    // ConfigureStakingAccount(delegate_id, authorized_voter_id)
    ConfigureStakingAccount(Option<Pubkey>, Option<Pubkey>),
    CreateStakingAccount(Pubkey, u64),
    Deploy(String),
    GetTransactionCount,
    // Pay(lamports, to, timestamp, timestamp_pubkey, witness(es), cancelable)
    Pay(
        u64,
        Pubkey,
        Option<DateTime<Utc>>,
        Option<Pubkey>,
        Option<Vec<Pubkey>>,
        Option<Pubkey>,
    ),
    // TimeElapsed(to, process_id, timestamp)
    TimeElapsed(Pubkey, Pubkey, DateTime<Utc>),
    // Witness(to, process_id)
    Witness(Pubkey, Pubkey),
}

#[derive(Debug, Clone)]
pub enum WalletError {
    CommandNotRecognized(String),
    BadParameter(String),
    DynamicProgramError(String),
    RpcRequestError(String),
}

impl fmt::Display for WalletError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid")
    }
}

impl error::Error for WalletError {
    fn description(&self) -> &str {
        "invalid"
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

pub struct WalletConfig {
    pub id: Keypair,
    pub command: WalletCommand,
    pub drone_host: Option<IpAddr>,
    pub drone_port: u16,
    pub host: IpAddr,
    pub rpc_client: Option<RpcClient>,
    pub rpc_host: Option<IpAddr>,
    pub rpc_port: u16,
    pub rpc_tls: bool,
}

impl Default for WalletConfig {
    fn default() -> WalletConfig {
        WalletConfig {
            command: WalletCommand::Balance,
            drone_host: None,
            drone_port: DRONE_PORT,
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            id: Keypair::new(),
            rpc_client: None,
            rpc_host: None,
            rpc_port: RPC_PORT,
            rpc_tls: false,
        }
    }
}

impl WalletConfig {
    pub fn drone_addr(&self) -> SocketAddr {
        SocketAddr::new(self.drone_host.unwrap_or(self.host), self.drone_port)
    }

    pub fn rpc_addr(&self) -> String {
        get_rpc_request_str(
            SocketAddr::new(self.rpc_host.unwrap_or(self.host), self.rpc_port),
            self.rpc_tls,
        )
    }
}

pub fn parse_command(
    pubkey: &Pubkey,
    matches: &ArgMatches<'_>,
) -> Result<WalletCommand, Box<dyn error::Error>> {
    let response = match matches.subcommand() {
        ("address", Some(_address_matches)) => Ok(WalletCommand::Address),
        ("airdrop", Some(airdrop_matches)) => {
            let lamports = airdrop_matches.value_of("lamports").unwrap().parse()?;
            Ok(WalletCommand::Airdrop(lamports))
        }
        ("balance", Some(_balance_matches)) => Ok(WalletCommand::Balance),
        ("cancel", Some(cancel_matches)) => {
            let pubkey_vec = bs58::decode(cancel_matches.value_of("process_id").unwrap())
                .into_vec()
                .expect("base58-encoded public key");

            if pubkey_vec.len() != mem::size_of::<Pubkey>() {
                eprintln!("{}", cancel_matches.usage());
                Err(WalletError::BadParameter("Invalid public key".to_string()))?;
            }
            let process_id = Pubkey::new(&pubkey_vec);
            Ok(WalletCommand::Cancel(process_id))
        }
        ("confirm", Some(confirm_matches)) => {
            let signatures = bs58::decode(confirm_matches.value_of("signature").unwrap())
                .into_vec()
                .expect("base58-encoded signature");

            if signatures.len() == mem::size_of::<Signature>() {
                let signature = Signature::new(&signatures);
                Ok(WalletCommand::Confirm(signature))
            } else {
                eprintln!("{}", confirm_matches.usage());
                Err(WalletError::BadParameter("Invalid signature".to_string()))
            }
        }
        ("configure-staking-account", Some(staking_config_matches)) => {
            let delegate_id = staking_config_matches
                .value_of("delegate")
                .map(|pubkey_string| {
                    let pubkey_vec = bs58::decode(pubkey_string)
                        .into_vec()
                        .expect("base58-encoded public key");
                    // TODO: Add valid pubkey check
                    Pubkey::new(&pubkey_vec)
                });
            let authorized_voter_id =
                staking_config_matches
                    .value_of("authorize")
                    .map(|pubkey_string| {
                        let pubkey_vec = bs58::decode(pubkey_string)
                            .into_vec()
                            .expect("base58-encoded public key");
                        // TODO: Add valid pubkey check
                        Pubkey::new(&pubkey_vec)
                    });
            Ok(WalletCommand::ConfigureStakingAccount(
                delegate_id,
                authorized_voter_id,
            ))
        }
        ("create-staking-account", Some(staking_matches)) => {
            let voting_account_string = staking_matches.value_of("voting_account_id").unwrap();
            let voting_account_vec = bs58::decode(voting_account_string)
                .into_vec()
                .expect("base58-encoded public key");

            if voting_account_vec.len() != mem::size_of::<Pubkey>() {
                eprintln!("{}", staking_matches.usage());
                Err(WalletError::BadParameter("Invalid public key".to_string()))?;
            }
            let voting_account_id = Pubkey::new(&voting_account_vec);
            let lamports = staking_matches.value_of("lamports").unwrap().parse()?;
            Ok(WalletCommand::CreateStakingAccount(
                voting_account_id,
                lamports,
            ))
        }
        ("deploy", Some(deploy_matches)) => Ok(WalletCommand::Deploy(
            deploy_matches
                .value_of("program_location")
                .unwrap()
                .to_string(),
        )),
        ("get-transaction-count", Some(_matches)) => Ok(WalletCommand::GetTransactionCount),
        ("pay", Some(pay_matches)) => {
            let lamports = pay_matches.value_of("lamports").unwrap().parse()?;
            let to = if pay_matches.is_present("to") {
                let pubkey_vec = bs58::decode(pay_matches.value_of("to").unwrap())
                    .into_vec()
                    .expect("base58-encoded public key");

                if pubkey_vec.len() != mem::size_of::<Pubkey>() {
                    eprintln!("{}", pay_matches.usage());
                    Err(WalletError::BadParameter(
                        "Invalid to public key".to_string(),
                    ))?;
                }
                Pubkey::new(&pubkey_vec)
            } else {
                *pubkey
            };
            let timestamp = if pay_matches.is_present("timestamp") {
                // Parse input for serde_json
                let date_string = if !pay_matches.value_of("timestamp").unwrap().contains('Z') {
                    format!("\"{}Z\"", pay_matches.value_of("timestamp").unwrap())
                } else {
                    format!("\"{}\"", pay_matches.value_of("timestamp").unwrap())
                };
                Some(serde_json::from_str(&date_string)?)
            } else {
                None
            };
            let timestamp_pubkey = if pay_matches.is_present("timestamp_pubkey") {
                let pubkey_vec = bs58::decode(pay_matches.value_of("timestamp_pubkey").unwrap())
                    .into_vec()
                    .expect("base58-encoded public key");

                if pubkey_vec.len() != mem::size_of::<Pubkey>() {
                    eprintln!("{}", pay_matches.usage());
                    Err(WalletError::BadParameter(
                        "Invalid timestamp public key".to_string(),
                    ))?;
                }
                Some(Pubkey::new(&pubkey_vec))
            } else {
                None
            };
            let witness_vec = if pay_matches.is_present("witness") {
                let witnesses = pay_matches.values_of("witness").unwrap();
                let mut collection = Vec::new();
                for witness in witnesses {
                    let pubkey_vec = bs58::decode(witness)
                        .into_vec()
                        .expect("base58-encoded public key");

                    if pubkey_vec.len() != mem::size_of::<Pubkey>() {
                        eprintln!("{}", pay_matches.usage());
                        Err(WalletError::BadParameter(
                            "Invalid witness public key".to_string(),
                        ))?;
                    }
                    collection.push(Pubkey::new(&pubkey_vec));
                }
                Some(collection)
            } else {
                None
            };
            let cancelable = if pay_matches.is_present("cancelable") {
                Some(*pubkey)
            } else {
                None
            };

            Ok(WalletCommand::Pay(
                lamports,
                to,
                timestamp,
                timestamp_pubkey,
                witness_vec,
                cancelable,
            ))
        }
        ("send-signature", Some(sig_matches)) => {
            let pubkey_vec = bs58::decode(sig_matches.value_of("to").unwrap())
                .into_vec()
                .expect("base58-encoded public key");

            if pubkey_vec.len() != mem::size_of::<Pubkey>() {
                eprintln!("{}", sig_matches.usage());
                Err(WalletError::BadParameter("Invalid public key".to_string()))?;
            }
            let to = Pubkey::new(&pubkey_vec);

            let pubkey_vec = bs58::decode(sig_matches.value_of("process_id").unwrap())
                .into_vec()
                .expect("base58-encoded public key");

            if pubkey_vec.len() != mem::size_of::<Pubkey>() {
                eprintln!("{}", sig_matches.usage());
                Err(WalletError::BadParameter("Invalid public key".to_string()))?;
            }
            let process_id = Pubkey::new(&pubkey_vec);
            Ok(WalletCommand::Witness(to, process_id))
        }
        ("send-timestamp", Some(timestamp_matches)) => {
            let pubkey_vec = bs58::decode(timestamp_matches.value_of("to").unwrap())
                .into_vec()
                .expect("base58-encoded public key");

            if pubkey_vec.len() != mem::size_of::<Pubkey>() {
                eprintln!("{}", timestamp_matches.usage());
                Err(WalletError::BadParameter("Invalid public key".to_string()))?;
            }
            let to = Pubkey::new(&pubkey_vec);

            let pubkey_vec = bs58::decode(timestamp_matches.value_of("process_id").unwrap())
                .into_vec()
                .expect("base58-encoded public key");

            if pubkey_vec.len() != mem::size_of::<Pubkey>() {
                eprintln!("{}", timestamp_matches.usage());
                Err(WalletError::BadParameter("Invalid public key".to_string()))?;
            }
            let process_id = Pubkey::new(&pubkey_vec);
            let dt = if timestamp_matches.is_present("datetime") {
                // Parse input for serde_json
                let date_string = if !timestamp_matches
                    .value_of("datetime")
                    .unwrap()
                    .contains('Z')
                {
                    format!("\"{}Z\"", timestamp_matches.value_of("datetime").unwrap())
                } else {
                    format!("\"{}\"", timestamp_matches.value_of("datetime").unwrap())
                };
                serde_json::from_str(&date_string)?
            } else {
                Utc::now()
            };
            Ok(WalletCommand::TimeElapsed(to, process_id, dt))
        }
        ("", None) => {
            eprintln!("{}", matches.usage());
            Err(WalletError::CommandNotRecognized(
                "no subcommand given".to_string(),
            ))
        }
        _ => unreachable!(),
    }?;
    Ok(response)
}

type ProcessResult = Result<String, Box<dyn error::Error>>;

fn process_airdrop(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    drone_addr: SocketAddr,
    lamports: u64,
) -> ProcessResult {
    println!(
        "Requesting airdrop of {:?} lamports from {}",
        lamports, drone_addr
    );
    let previous_balance = match rpc_client.retry_get_balance(1, &config.id.pubkey(), 5)? {
        Some(lamports) => lamports,
        None => Err(WalletError::RpcRequestError(
            "Received result of an unexpected type".to_string(),
        ))?,
    };

    request_and_confirm_airdrop(&rpc_client, &drone_addr, &config.id.pubkey(), lamports)?;

    let current_balance = rpc_client
        .retry_get_balance(1, &config.id.pubkey(), 5)?
        .unwrap_or(previous_balance);

    if current_balance < previous_balance {
        Err(format!(
            "Airdrop failed: current_balance({}) < previous_balance({})",
            current_balance, previous_balance
        ))?;
    }
    if current_balance - previous_balance < lamports {
        Err(format!(
            "Airdrop failed: Account balance increased by {} instead of {}",
            current_balance - previous_balance,
            lamports
        ))?;
    }
    Ok(format!("Your balance is: {:?}", current_balance))
}

fn process_balance(config: &WalletConfig, rpc_client: &RpcClient) -> ProcessResult {
    let balance = rpc_client.retry_get_balance(1, &config.id.pubkey(), 5)?;
    match balance {
        Some(0) => Ok("No account found! Request an airdrop to get started.".to_string()),
        Some(lamports) => Ok(format!("Your balance is: {:?}", lamports)),
        None => Err(WalletError::RpcRequestError(
            "Received result of an unexpected type".to_string(),
        ))?,
    }
}

fn process_confirm(rpc_client: &RpcClient, signature: Signature) -> ProcessResult {
    let params = json!([format!("{}", signature)]);
    let confirmation = rpc_client
        .retry_make_rpc_request(1, &RpcRequest::ConfirmTransaction, Some(params), 5)?
        .as_bool();
    match confirmation {
        Some(b) => {
            if b {
                Ok("Confirmed".to_string())
            } else {
                Ok("Not found".to_string())
            }
        }
        None => Err(WalletError::RpcRequestError(
            "Received result of an unexpected type".to_string(),
        ))?,
    }
}

fn process_configure_staking(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    delegate_option: Option<Pubkey>,
    authorized_voter_option: Option<Pubkey>,
) -> ProcessResult {
    let recent_blockhash = get_recent_blockhash(&rpc_client)?;
    let mut ixs = vec![];
    if let Some(delegate_id) = delegate_option {
        ixs.push(VoteInstruction::new_delegate_stake(
            &config.id.pubkey(),
            &delegate_id,
        ));
    }
    if let Some(authorized_voter_id) = authorized_voter_option {
        ixs.push(VoteInstruction::new_authorize_voter(
            &config.id.pubkey(),
            &authorized_voter_id,
        ));
    }
    let mut tx = Transaction::new(ixs);
    tx.sign(&[&config.id], recent_blockhash);
    let signature_str = send_and_confirm_transaction(&rpc_client, &mut tx, &config.id)?;
    Ok(signature_str.to_string())
}

fn process_create_staking(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    voting_account_id: &Pubkey,
    lamports: u64,
) -> ProcessResult {
    let recent_blockhash = get_recent_blockhash(&rpc_client)?;
    let mut tx =
        VoteTransaction::new_account(&config.id, voting_account_id, recent_blockhash, lamports, 0);
    let signature_str = send_and_confirm_transaction(&rpc_client, &mut tx, &config.id)?;
    Ok(signature_str.to_string())
}

fn process_deploy(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    program_location: &str,
) -> ProcessResult {
    let balance = rpc_client.retry_get_balance(1, &config.id.pubkey(), 5)?;
    if let Some(lamports) = balance {
        if lamports < 1 {
            Err(WalletError::DynamicProgramError(
                "Insufficient funds".to_string(),
            ))?
        }
    }

    let blockhash = get_recent_blockhash(&rpc_client)?;
    let program_id = Keypair::new();
    let mut file = File::open(program_location).map_err(|err| {
        WalletError::DynamicProgramError(
            format!("Unable to open program file: {}", err).to_string(),
        )
    })?;
    let mut program_data = Vec::new();
    file.read_to_end(&mut program_data).map_err(|err| {
        WalletError::DynamicProgramError(
            format!("Unable to read program file: {}", err).to_string(),
        )
    })?;

    let mut tx = SystemTransaction::new_program_account(
        &config.id,
        &program_id.pubkey(),
        blockhash,
        1,
        program_data.len() as u64,
        &bpf_loader::id(),
        0,
    );
    trace!("Creating program account");
    send_and_confirm_transaction(&rpc_client, &mut tx, &config.id).map_err(|_| {
        WalletError::DynamicProgramError("Program allocate space failed".to_string())
    })?;

    trace!("Writing program data");
    let write_transactions: Vec<_> = program_data
        .chunks(USERDATA_CHUNK_SIZE)
        .zip(0..)
        .map(|(chunk, i)| {
            LoaderTransaction::new_write(
                &program_id,
                &bpf_loader::id(),
                (i * USERDATA_CHUNK_SIZE) as u32,
                chunk.to_vec(),
                blockhash,
                0,
            )
        })
        .collect();
    send_and_confirm_transactions(&rpc_client, write_transactions, &program_id)?;

    trace!("Finalizing program account");
    let mut tx = LoaderTransaction::new_finalize(&program_id, &bpf_loader::id(), blockhash, 0);
    send_and_confirm_transaction(&rpc_client, &mut tx, &program_id).map_err(|_| {
        WalletError::DynamicProgramError("Program finalize transaction failed".to_string())
    })?;

    Ok(json!({
        "programId": format!("{}", program_id.pubkey()),
    })
    .to_string())
}

fn process_pay(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    lamports: u64,
    to: &Pubkey,
    timestamp: Option<DateTime<Utc>>,
    timestamp_pubkey: Option<Pubkey>,
    witnesses: &Option<Vec<Pubkey>>,
    cancelable: Option<Pubkey>,
) -> ProcessResult {
    let blockhash = get_recent_blockhash(&rpc_client)?;

    if timestamp == None && *witnesses == None {
        let mut tx = SystemTransaction::new_move(&config.id, to, lamports, blockhash, 0);
        let signature_str = send_and_confirm_transaction(&rpc_client, &mut tx, &config.id)?;
        Ok(signature_str.to_string())
    } else if *witnesses == None {
        let dt = timestamp.unwrap();
        let dt_pubkey = match timestamp_pubkey {
            Some(pubkey) => pubkey,
            None => config.id.pubkey(),
        };

        let contract_state = Keypair::new();

        // Initializing contract
        let mut tx = BudgetTransaction::new_on_date(
            &config.id,
            to,
            &contract_state.pubkey(),
            dt,
            &dt_pubkey,
            cancelable,
            lamports,
            blockhash,
        );
        let signature_str = send_and_confirm_transaction(&rpc_client, &mut tx, &config.id)?;

        Ok(json!({
            "signature": signature_str,
            "processId": format!("{}", contract_state.pubkey()),
        })
        .to_string())
    } else if timestamp == None {
        let blockhash = get_recent_blockhash(&rpc_client)?;

        let witness = if let Some(ref witness_vec) = *witnesses {
            witness_vec[0]
        } else {
            Err(WalletError::BadParameter(
                "Could not parse required signature pubkey(s)".to_string(),
            ))?
        };

        let contract_state = Keypair::new();

        // Initializing contract
        let mut tx = BudgetTransaction::new_when_signed(
            &config.id,
            to,
            &contract_state.pubkey(),
            &witness,
            cancelable,
            lamports,
            blockhash,
        );
        let signature_str = send_and_confirm_transaction(&rpc_client, &mut tx, &config.id)?;

        Ok(json!({
            "signature": signature_str,
            "processId": format!("{}", contract_state.pubkey()),
        })
        .to_string())
    } else {
        Ok("Combo transactions not yet handled".to_string())
    }
}

fn process_cancel(rpc_client: &RpcClient, config: &WalletConfig, pubkey: &Pubkey) -> ProcessResult {
    let blockhash = get_recent_blockhash(&rpc_client)?;
    let mut tx =
        BudgetTransaction::new_signature(&config.id, pubkey, &config.id.pubkey(), blockhash);
    let signature_str = send_and_confirm_transaction(&rpc_client, &mut tx, &config.id)?;
    Ok(signature_str.to_string())
}

fn process_get_transaction_count(rpc_client: &RpcClient) -> ProcessResult {
    let transaction_count = rpc_client
        .retry_make_rpc_request(1, &RpcRequest::GetTransactionCount, None, 5)?
        .as_u64();
    match transaction_count {
        Some(count) => Ok(count.to_string()),
        None => Err(WalletError::RpcRequestError(
            "Received result of an unexpected type".to_string(),
        ))?,
    }
}

fn process_time_elapsed(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    drone_addr: SocketAddr,
    to: &Pubkey,
    pubkey: &Pubkey,
    dt: DateTime<Utc>,
) -> ProcessResult {
    let balance = rpc_client.retry_get_balance(1, &config.id.pubkey(), 5)?;

    if let Some(0) = balance {
        request_and_confirm_airdrop(&rpc_client, &drone_addr, &config.id.pubkey(), 1)?;
    }

    let blockhash = get_recent_blockhash(&rpc_client)?;

    let mut tx = BudgetTransaction::new_timestamp(&config.id, pubkey, to, dt, blockhash);
    let signature_str = send_and_confirm_transaction(&rpc_client, &mut tx, &config.id)?;

    Ok(signature_str.to_string())
}

fn process_witness(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    drone_addr: SocketAddr,
    to: &Pubkey,
    pubkey: &Pubkey,
) -> ProcessResult {
    let balance = rpc_client.retry_get_balance(1, &config.id.pubkey(), 5)?;

    if let Some(0) = balance {
        request_and_confirm_airdrop(&rpc_client, &drone_addr, &config.id.pubkey(), 1)?;
    }

    let blockhash = get_recent_blockhash(&rpc_client)?;
    let mut tx = BudgetTransaction::new_signature(&config.id, pubkey, to, blockhash);
    let signature_str = send_and_confirm_transaction(&rpc_client, &mut tx, &config.id)?;

    Ok(signature_str.to_string())
}

pub fn process_command(config: &WalletConfig) -> ProcessResult {
    if let WalletCommand::Address = config.command {
        // Get address of this client
        return Ok(format!("{}", config.id.pubkey()));
    }

    let drone_addr = config.drone_addr();
    let rpc_client = if config.rpc_client.is_none() {
        let rpc_addr = config.rpc_addr();
        RpcClient::new(rpc_addr)
    } else {
        // Primarily for testing
        config.rpc_client.clone().unwrap()
    };

    match config.command {
        // Get address of this client
        WalletCommand::Address => unreachable!(),

        // Request an airdrop from Solana Drone;
        WalletCommand::Airdrop(lamports) => {
            process_airdrop(&rpc_client, config, drone_addr, lamports)
        }

        // Check client balance
        WalletCommand::Balance => process_balance(config, &rpc_client),

        // Cancel a contract by contract Pubkey
        WalletCommand::Cancel(pubkey) => process_cancel(&rpc_client, config, &pubkey),

        // Confirm the last client transaction by signature
        WalletCommand::Confirm(signature) => process_confirm(&rpc_client, signature),

        // Configure staking account already created
        WalletCommand::ConfigureStakingAccount(delegate_option, authorized_voter_option) => {
            process_configure_staking(
                &rpc_client,
                config,
                delegate_option,
                authorized_voter_option,
            )
        }

        // Create staking account
        WalletCommand::CreateStakingAccount(voting_account_id, lamports) => {
            process_create_staking(&rpc_client, config, &voting_account_id, lamports)
        }

        // Deploy a custom program to the chain
        WalletCommand::Deploy(ref program_location) => {
            process_deploy(&rpc_client, config, program_location)
        }

        WalletCommand::GetTransactionCount => process_get_transaction_count(&rpc_client),

        // If client has positive balance, pay lamports to another address
        WalletCommand::Pay(
            lamports,
            to,
            timestamp,
            timestamp_pubkey,
            ref witnesses,
            cancelable,
        ) => process_pay(
            &rpc_client,
            config,
            lamports,
            &to,
            timestamp,
            timestamp_pubkey,
            witnesses,
            cancelable,
        ),

        // Apply time elapsed to contract
        WalletCommand::TimeElapsed(to, pubkey, dt) => {
            process_time_elapsed(&rpc_client, config, drone_addr, &to, &pubkey, dt)
        }

        // Apply witness signature to contract
        WalletCommand::Witness(to, pubkey) => {
            process_witness(&rpc_client, config, drone_addr, &to, &pubkey)
        }
    }
}

fn get_recent_blockhash(rpc_client: &RpcClient) -> Result<Hash, Box<dyn error::Error>> {
    let result = rpc_client.retry_make_rpc_request(1, &RpcRequest::GetRecentBlockhash, None, 5)?;
    if result.as_str().is_none() {
        Err(WalletError::RpcRequestError(
            "Received bad blockhash".to_string(),
        ))?
    }
    let blockhash_str = result.as_str().unwrap();
    let blockhash_vec = bs58::decode(blockhash_str)
        .into_vec()
        .map_err(|_| WalletError::RpcRequestError("Received bad blockhash".to_string()))?;
    Ok(Hash::new(&blockhash_vec))
}

fn get_next_blockhash(
    rpc_client: &RpcClient,
    previous_blockhash: &Hash,
) -> Result<Hash, Box<dyn error::Error>> {
    let mut next_blockhash_retries = 3;
    loop {
        let next_blockhash = get_recent_blockhash(rpc_client)?;
        if cfg!(not(test)) {
            if next_blockhash != *previous_blockhash {
                return Ok(next_blockhash);
            }
        } else {
            // When using MockRpcClient, get_recent_blockhash() returns a constant value
            return Ok(next_blockhash);
        }
        if next_blockhash_retries == 0 {
            Err(WalletError::RpcRequestError(
                format!(
                    "Unable to fetch new blockhash, blockhash stuck at {:?}",
                    next_blockhash
                )
                .to_string(),
            ))?;
        }
        next_blockhash_retries -= 1;
        // Retry ~twice during a slot
        sleep(Duration::from_millis(
            500 * DEFAULT_TICKS_PER_SLOT / NUM_TICKS_PER_SECOND,
        ));
    }
}

fn send_transaction(
    rpc_client: &RpcClient,
    transaction: &Transaction,
) -> Result<String, Box<dyn error::Error>> {
    let serialized = serialize(transaction).unwrap();
    let params = json!([serialized]);
    let signature =
        rpc_client.retry_make_rpc_request(2, &RpcRequest::SendTransaction, Some(params), 5)?;
    if signature.as_str().is_none() {
        Err(WalletError::RpcRequestError(
            "Received result of an unexpected type".to_string(),
        ))?
    }
    Ok(signature.as_str().unwrap().to_string())
}

fn confirm_transaction(
    rpc_client: &RpcClient,
    signature: &str,
) -> Result<RpcSignatureStatus, Box<dyn error::Error>> {
    let params = json!([signature.to_string()]);
    let signature_status =
        rpc_client.retry_make_rpc_request(1, &RpcRequest::GetSignatureStatus, Some(params), 5)?;
    if let Some(status) = signature_status.as_str() {
        let rpc_status = RpcSignatureStatus::from_str(status).map_err(|_| {
            WalletError::RpcRequestError("Unable to parse signature status".to_string())
        })?;
        Ok(rpc_status)
    } else {
        Err(WalletError::RpcRequestError(
            "Received result of an unexpected type".to_string(),
        ))?
    }
}

fn send_and_confirm_transaction<T: KeypairUtil>(
    rpc_client: &RpcClient,
    transaction: &mut Transaction,
    signer: &T,
) -> Result<String, Box<dyn error::Error>> {
    let mut send_retries = 5;
    loop {
        let mut status_retries = 4;
        let signature_str = send_transaction(rpc_client, transaction)?;
        let status = loop {
            let status = confirm_transaction(rpc_client, &signature_str)?;
            if status == RpcSignatureStatus::SignatureNotFound {
                status_retries -= 1;
                if status_retries == 0 {
                    break status;
                }
            } else {
                break status;
            }
            if cfg!(not(test)) {
                // Retry ~twice during a slot
                sleep(Duration::from_millis(
                    500 * DEFAULT_TICKS_PER_SLOT / NUM_TICKS_PER_SECOND,
                ));
            }
        };
        match status {
            RpcSignatureStatus::AccountInUse | RpcSignatureStatus::SignatureNotFound => {
                // Fetch a new blockhash and re-sign the transaction before sending it again
                resign_transaction(rpc_client, transaction, signer)?;
                send_retries -= 1;
            }
            RpcSignatureStatus::Confirmed => {
                return Ok(signature_str);
            }
            _ => {
                send_retries = 0;
            }
        }
        if send_retries == 0 {
            Err(WalletError::RpcRequestError(format!(
                "Transaction {:?} failed: {:?}",
                signature_str, status
            )))?;
        }
    }
}

fn send_and_confirm_transactions(
    rpc_client: &RpcClient,
    mut transactions: Vec<Transaction>,
    signer: &Keypair,
) -> Result<(), Box<dyn error::Error>> {
    let mut send_retries = 5;
    loop {
        let mut status_retries = 4;

        // Send all transactions
        let mut transactions_signatures = vec![];
        for transaction in transactions {
            if cfg!(not(test)) {
                // Delay ~1 tick between write transactions in an attempt to reduce AccountInUse errors
                // since all the write transactions modify the same program account
                sleep(Duration::from_millis(1000 / NUM_TICKS_PER_SECOND));
            }

            let signature = send_transaction(&rpc_client, &transaction).ok();
            transactions_signatures.push((transaction, signature))
        }

        // Collect statuses for all the transactions, drop those that are confirmed
        while status_retries > 0 {
            status_retries -= 1;

            if cfg!(not(test)) {
                // Retry ~twice during a slot
                sleep(Duration::from_millis(
                    500 * DEFAULT_TICKS_PER_SLOT / NUM_TICKS_PER_SECOND,
                ));
            }

            transactions_signatures = transactions_signatures
                .into_iter()
                .filter(|(_transaction, signature)| {
                    if let Some(signature) = signature {
                        if let Ok(status) = confirm_transaction(rpc_client, &signature) {
                            return status != RpcSignatureStatus::Confirmed;
                        }
                    }
                    true
                })
                .collect();

            if transactions_signatures.is_empty() {
                return Ok(());
            }
        }

        if send_retries == 0 {
            Err(WalletError::RpcRequestError(
                "Transactions failed".to_string(),
            ))?;
        }
        send_retries -= 1;

        // Re-sign any failed transactions with a new blockhash and retry
        let blockhash =
            get_next_blockhash(rpc_client, &transactions_signatures[0].0.recent_blockhash)?;
        transactions = transactions_signatures
            .into_iter()
            .map(|(mut transaction, _)| {
                transaction.sign(&[signer], blockhash);
                transaction
            })
            .collect();
    }
}

fn resign_transaction<T: KeypairUtil>(
    rpc_client: &RpcClient,
    tx: &mut Transaction,
    signer_key: &T,
) -> Result<(), Box<dyn error::Error>> {
    let blockhash = get_next_blockhash(rpc_client, &tx.recent_blockhash)?;
    tx.sign(&[signer_key], blockhash);
    Ok(())
}

// Quick and dirty Keypair that assumes the client will do retries but not update the
// blockhash. If the client updates the blockhash, the signature will be invalid.
// TODO: Parse `msg` and use that data to make a new airdrop request.
struct DroneKeypair {
    transaction: Transaction,
}

impl DroneKeypair {
    fn new_keypair(
        drone_addr: &SocketAddr,
        to_pubkey: &Pubkey,
        lamports: u64,
        blockhash: Hash,
    ) -> Result<Self, Box<dyn error::Error>> {
        let transaction = request_airdrop_transaction(drone_addr, to_pubkey, lamports, blockhash)?;
        Ok(Self { transaction })
    }

    fn airdrop_transaction(&self) -> Transaction {
        self.transaction.clone()
    }
}

impl KeypairUtil for DroneKeypair {
    fn new() -> Self {
        unimplemented!();
    }

    /// Return the public key of the keypair used to sign votes
    fn pubkey(&self) -> Pubkey {
        self.transaction.account_keys[0]
    }

    fn sign_message(&self, _msg: &[u8]) -> Signature {
        self.transaction.signatures[0]
    }
}

pub fn request_and_confirm_airdrop(
    rpc_client: &RpcClient,
    drone_addr: &SocketAddr,
    to_pubkey: &Pubkey,
    lamports: u64,
) -> Result<(), Box<dyn error::Error>> {
    let blockhash = get_recent_blockhash(rpc_client)?;
    let keypair = DroneKeypair::new_keypair(drone_addr, to_pubkey, lamports, blockhash)?;
    let mut tx = keypair.airdrop_transaction();
    send_and_confirm_transaction(rpc_client, &mut tx, &keypair)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{App, Arg, ArgGroup, SubCommand};
    use serde_json::Value;
    use solana::socketaddr;
    use solana_client::rpc_mock::{PUBKEY, SIGNATURE};
    use solana_sdk::signature::{gen_keypair_file, read_keypair, read_pkcs8, Keypair, KeypairUtil};
    use std::fs;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::path::{Path, PathBuf};

    #[test]
    fn test_wallet_config_drone_addr() {
        let mut config = WalletConfig::default();
        assert_eq!(
            config.drone_addr(),
            SocketAddr::new(config.host, config.drone_port)
        );

        config.drone_port = 1234;
        assert_eq!(config.drone_addr(), SocketAddr::new(config.host, 1234));

        config.drone_host = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
        assert_eq!(
            config.drone_addr(),
            SocketAddr::new(config.drone_host.unwrap(), 1234)
        );
    }

    #[test]
    fn test_wallet_config_rpc_addr() {
        let mut config = WalletConfig::default();
        assert_eq!(config.rpc_addr(), "http://127.0.0.1:8899");
        config.rpc_port = 1234;
        assert_eq!(config.rpc_addr(), "http://127.0.0.1:1234");
        config.rpc_host = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
        assert_eq!(config.rpc_addr(), "http://127.0.0.2:1234");
    }

    #[test]
    fn test_wallet_parse_command() {
        let test_commands = App::new("test")
            .subcommand(SubCommand::with_name("address").about("Get your public key"))
            .subcommand(
                SubCommand::with_name("airdrop")
                    .about("Request a batch of lamports")
                    .arg(
                        Arg::with_name("lamports")
                            .index(1)
                            .value_name("NUM")
                            .takes_value(true)
                            .required(true)
                            .help("The number of lamports to request"),
                    ),
            )
            .subcommand(SubCommand::with_name("balance").about("Get your balance"))
            .subcommand(
                SubCommand::with_name("cancel")
                    .about("Cancel a transfer")
                    .arg(
                        Arg::with_name("process_id")
                            .index(1)
                            .value_name("PROCESS_ID")
                            .takes_value(true)
                            .required(true)
                            .help("The process id of the transfer to cancel"),
                    ),
            )
            .subcommand(
                SubCommand::with_name("confirm")
                    .about("Confirm transaction by signature")
                    .arg(
                        Arg::with_name("signature")
                            .index(1)
                            .value_name("SIGNATURE")
                            .takes_value(true)
                            .required(true)
                            .help("The transaction signature to confirm"),
                    ),
            )
            .subcommand(
                SubCommand::with_name("configure-staking-account")
                    .about("Configure staking account for node")
                    .group(
                        ArgGroup::with_name("options")
                            .args(&["delegate", "authorize"])
                            .multiple(true)
                            .required(true),
                    )
                    .arg(
                        Arg::with_name("delegate")
                            .long("delegate-account")
                            .value_name("PUBKEY")
                            .takes_value(true)
                            .help("Address to delegate this vote account to"),
                    )
                    .arg(
                        Arg::with_name("authorize")
                            .long("authorize-voter")
                            .value_name("PUBKEY")
                            .takes_value(true)
                            .help("Vote signer to authorize"),
                    ),
            )
            .subcommand(
                SubCommand::with_name("create-staking-account")
                    .about("Create staking account for node")
                    .arg(
                        Arg::with_name("voting_account_id")
                            .index(1)
                            .value_name("PUBKEY")
                            .takes_value(true)
                            .required(true)
                            .help("Staking account address to fund"),
                    )
                    .arg(
                        Arg::with_name("lamports")
                            .index(2)
                            .value_name("NUM")
                            .takes_value(true)
                            .required(true)
                            .help("The number of lamports to send to staking account"),
                    ),
            )
            .subcommand(
                SubCommand::with_name("deploy")
                    .about("Deploy a program")
                    .arg(
                        Arg::with_name("program_location")
                            .index(1)
                            .value_name("PATH")
                            .takes_value(true)
                            .required(true)
                            .help("/path/to/program.o"),
                    ), // TODO: Add "loader" argument; current default is bpf_loader
            )
            .subcommand(
                SubCommand::with_name("get-transaction-count")
                    .about("Get current transaction count"),
            )
            .subcommand(
                SubCommand::with_name("pay")
                    .about("Send a payment")
                    .arg(
                        Arg::with_name("to")
                            .index(1)
                            .value_name("PUBKEY")
                            .takes_value(true)
                            .required(true)
                            .help("The pubkey of recipient"),
                    )
                    .arg(
                        Arg::with_name("lamports")
                            .index(2)
                            .value_name("NUM")
                            .takes_value(true)
                            .required(true)
                            .help("The number of lamports to send"),
                    )
                    .arg(
                        Arg::with_name("timestamp")
                            .long("after")
                            .value_name("DATETIME")
                            .takes_value(true)
                            .help("A timestamp after which transaction will execute"),
                    )
                    .arg(
                        Arg::with_name("timestamp_pubkey")
                            .long("require-timestamp-from")
                            .value_name("PUBKEY")
                            .takes_value(true)
                            .requires("timestamp")
                            .help("Require timestamp from this third party"),
                    )
                    .arg(
                        Arg::with_name("witness")
                            .long("require-signature-from")
                            .value_name("PUBKEY")
                            .takes_value(true)
                            .multiple(true)
                            .use_delimiter(true)
                            .help("Any third party signatures required to unlock the lamports"),
                    )
                    .arg(
                        Arg::with_name("cancelable")
                            .long("cancelable")
                            .takes_value(false),
                    ),
            )
            .subcommand(
                SubCommand::with_name("send-signature")
                    .about("Send a signature to authorize a transfer")
                    .arg(
                        Arg::with_name("to")
                            .index(1)
                            .value_name("PUBKEY")
                            .takes_value(true)
                            .required(true)
                            .help("The pubkey of recipient"),
                    )
                    .arg(
                        Arg::with_name("process_id")
                            .index(2)
                            .value_name("PROCESS_ID")
                            .takes_value(true)
                            .required(true)
                            .help("The process id of the transfer to authorize"),
                    ),
            )
            .subcommand(
                SubCommand::with_name("send-timestamp")
                    .about("Send a timestamp to unlock a transfer")
                    .arg(
                        Arg::with_name("to")
                            .index(1)
                            .value_name("PUBKEY")
                            .takes_value(true)
                            .required(true)
                            .help("The pubkey of recipient"),
                    )
                    .arg(
                        Arg::with_name("process_id")
                            .index(2)
                            .value_name("PROCESS_ID")
                            .takes_value(true)
                            .required(true)
                            .help("The process id of the transfer to unlock"),
                    )
                    .arg(
                        Arg::with_name("datetime")
                            .long("date")
                            .value_name("DATETIME")
                            .takes_value(true)
                            .help("Optional arbitrary timestamp to apply"),
                    ),
            );
        let pubkey = Keypair::new().pubkey();
        let pubkey_string = format!("{}", pubkey);
        let witness0 = Keypair::new().pubkey();
        let witness0_string = format!("{}", witness0);
        let witness1 = Keypair::new().pubkey();
        let witness1_string = format!("{}", witness1);
        let dt = Utc.ymd(2018, 9, 19).and_hms(17, 30, 59);

        // Test Airdrop Subcommand
        let test_airdrop = test_commands
            .clone()
            .get_matches_from(vec!["test", "airdrop", "50"]);
        assert_eq!(
            parse_command(&pubkey, &test_airdrop).unwrap(),
            WalletCommand::Airdrop(50)
        );
        let test_bad_airdrop = test_commands
            .clone()
            .get_matches_from(vec!["test", "airdrop", "notint"]);
        assert!(parse_command(&pubkey, &test_bad_airdrop).is_err());

        // Test Cancel Subcommand
        let test_cancel =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "cancel", &pubkey_string]);
        assert_eq!(
            parse_command(&pubkey, &test_cancel).unwrap(),
            WalletCommand::Cancel(pubkey)
        );

        // Test Confirm Subcommand
        let signature = Signature::new(&vec![1; 64]);
        let signature_string = format!("{:?}", signature);
        let test_confirm =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "confirm", &signature_string]);
        assert_eq!(
            parse_command(&pubkey, &test_confirm).unwrap(),
            WalletCommand::Confirm(signature)
        );
        let test_bad_signature = test_commands
            .clone()
            .get_matches_from(vec!["test", "confirm", "deadbeef"]);
        assert!(parse_command(&pubkey, &test_bad_signature).is_err());

        // Test ConfigureStakingAccount Subcommand
        let second_pubkey = Keypair::new().pubkey();
        let second_pubkey_string = format!("{}", second_pubkey);
        let test_configure_staking_account = test_commands.clone().get_matches_from(vec![
            "test",
            "configure-staking-account",
            "--delegate-account",
            &pubkey_string,
            "--authorize-voter",
            &second_pubkey_string,
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_configure_staking_account).unwrap(),
            WalletCommand::ConfigureStakingAccount(Some(pubkey), Some(second_pubkey))
        );
        let test_configure_staking_account = test_commands.clone().get_matches_from(vec![
            "test",
            "configure-staking-account",
            "--delegate-account",
            &pubkey_string,
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_configure_staking_account).unwrap(),
            WalletCommand::ConfigureStakingAccount(Some(pubkey), None)
        );

        // Test CreateStakingAccount SubCommand
        let test_create_staking_account = test_commands.clone().get_matches_from(vec![
            "test",
            "create-staking-account",
            &pubkey_string,
            "50",
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_create_staking_account).unwrap(),
            WalletCommand::CreateStakingAccount(pubkey, 50)
        );
        let test_bad_pubkey = test_commands.clone().get_matches_from(vec![
            "test",
            "create-staking-account",
            "deadbeef",
            "50",
        ]);
        assert!(parse_command(&pubkey, &test_bad_pubkey).is_err());

        // Test Deploy Subcommand
        let test_deploy =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "deploy", "/Users/test/program.o"]);
        assert_eq!(
            parse_command(&pubkey, &test_deploy).unwrap(),
            WalletCommand::Deploy("/Users/test/program.o".to_string())
        );

        // Test Simple Pay Subcommand
        let test_pay =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "pay", &pubkey_string, "50"]);
        assert_eq!(
            parse_command(&pubkey, &test_pay).unwrap(),
            WalletCommand::Pay(50, pubkey, None, None, None, None)
        );
        let test_bad_pubkey = test_commands
            .clone()
            .get_matches_from(vec!["test", "pay", "deadbeef", "50"]);
        assert!(parse_command(&pubkey, &test_bad_pubkey).is_err());

        // Test Pay Subcommand w/ Witness
        let test_pay_multiple_witnesses = test_commands.clone().get_matches_from(vec![
            "test",
            "pay",
            &pubkey_string,
            "50",
            "--require-signature-from",
            &witness0_string,
            "--require-signature-from",
            &witness1_string,
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_pay_multiple_witnesses).unwrap(),
            WalletCommand::Pay(50, pubkey, None, None, Some(vec![witness0, witness1]), None)
        );
        let test_pay_single_witness = test_commands.clone().get_matches_from(vec![
            "test",
            "pay",
            &pubkey_string,
            "50",
            "--require-signature-from",
            &witness0_string,
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_pay_single_witness).unwrap(),
            WalletCommand::Pay(50, pubkey, None, None, Some(vec![witness0]), None)
        );

        // Test Pay Subcommand w/ Timestamp
        let test_pay_timestamp = test_commands.clone().get_matches_from(vec![
            "test",
            "pay",
            &pubkey_string,
            "50",
            "--after",
            "2018-09-19T17:30:59",
            "--require-timestamp-from",
            &witness0_string,
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_pay_timestamp).unwrap(),
            WalletCommand::Pay(50, pubkey, Some(dt), Some(witness0), None, None)
        );

        // Test Send-Signature Subcommand
        let test_send_signature = test_commands.clone().get_matches_from(vec![
            "test",
            "send-signature",
            &pubkey_string,
            &pubkey_string,
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_send_signature).unwrap(),
            WalletCommand::Witness(pubkey, pubkey)
        );
        let test_pay_multiple_witnesses = test_commands.clone().get_matches_from(vec![
            "test",
            "pay",
            &pubkey_string,
            "50",
            "--after",
            "2018-09-19T17:30:59",
            "--require-signature-from",
            &witness0_string,
            "--require-timestamp-from",
            &witness0_string,
            "--require-signature-from",
            &witness1_string,
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_pay_multiple_witnesses).unwrap(),
            WalletCommand::Pay(
                50,
                pubkey,
                Some(dt),
                Some(witness0),
                Some(vec![witness0, witness1]),
                None
            )
        );

        // Test Send-Timestamp Subcommand
        let test_send_timestamp = test_commands.clone().get_matches_from(vec![
            "test",
            "send-timestamp",
            &pubkey_string,
            &pubkey_string,
            "--date",
            "2018-09-19T17:30:59",
        ]);
        assert_eq!(
            parse_command(&pubkey, &test_send_timestamp).unwrap(),
            WalletCommand::TimeElapsed(pubkey, pubkey, dt)
        );
        let test_bad_timestamp = test_commands.clone().get_matches_from(vec![
            "test",
            "send-timestamp",
            &pubkey_string,
            &pubkey_string,
            "--date",
            "20180919T17:30:59",
        ]);
        assert!(parse_command(&pubkey, &test_bad_timestamp).is_err());
    }

    #[test]
    fn test_wallet_process_command() {
        // Success cases
        let mut config = WalletConfig::default();
        config.rpc_client = Some(RpcClient::new("succeeds".to_string()));

        let keypair = Keypair::new();
        let pubkey = keypair.pubkey().to_string();
        config.id = keypair;
        config.command = WalletCommand::Address;
        assert_eq!(process_command(&config).unwrap(), pubkey);

        config.command = WalletCommand::Balance;
        assert_eq!(process_command(&config).unwrap(), "Your balance is: 50");

        let process_id = Keypair::new().pubkey();
        config.command = WalletCommand::Cancel(process_id);
        assert_eq!(process_command(&config).unwrap(), SIGNATURE);

        let good_signature = Signature::new(&bs58::decode(SIGNATURE).into_vec().unwrap());
        config.command = WalletCommand::Confirm(good_signature);
        assert_eq!(process_command(&config).unwrap(), "Confirmed");
        let missing_signature = Signature::new(&bs58::decode("5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW").into_vec().unwrap());
        config.command = WalletCommand::Confirm(missing_signature);
        assert_eq!(process_command(&config).unwrap(), "Not found");

        let bob_pubkey = Keypair::new().pubkey();
        config.command = WalletCommand::ConfigureStakingAccount(None, Some(bob_pubkey));
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        config.command = WalletCommand::CreateStakingAccount(bob_pubkey, 10);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        config.command = WalletCommand::GetTransactionCount;
        assert_eq!(process_command(&config).unwrap(), "1234");

        config.command = WalletCommand::Pay(10, bob_pubkey, None, None, None, None);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let date_string = "\"2018-09-19T17:30:59Z\"";
        let dt: DateTime<Utc> = serde_json::from_str(&date_string).unwrap();
        config.command = WalletCommand::Pay(
            10,
            bob_pubkey,
            Some(dt),
            Some(config.id.pubkey()),
            None,
            None,
        );
        let result = process_command(&config);
        let json: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(
            json.as_object()
                .unwrap()
                .get("signature")
                .unwrap()
                .as_str()
                .unwrap(),
            SIGNATURE.to_string()
        );

        let witness = Keypair::new().pubkey();
        config.command = WalletCommand::Pay(
            10,
            bob_pubkey,
            None,
            None,
            Some(vec![witness]),
            Some(config.id.pubkey()),
        );
        let result = process_command(&config);
        let json: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(
            json.as_object()
                .unwrap()
                .get("signature")
                .unwrap()
                .as_str()
                .unwrap(),
            SIGNATURE.to_string()
        );

        let process_id = Keypair::new().pubkey();
        config.command = WalletCommand::TimeElapsed(bob_pubkey, process_id, dt);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let witness = Keypair::new().pubkey();
        config.command = WalletCommand::Witness(bob_pubkey, witness);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        // Need airdrop cases
        config.command = WalletCommand::Airdrop(50);
        assert!(process_command(&config).is_err());

        config.rpc_client = Some(RpcClient::new("airdrop".to_string()));
        config.command = WalletCommand::TimeElapsed(bob_pubkey, process_id, dt);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let witness = Keypair::new().pubkey();
        config.command = WalletCommand::Witness(bob_pubkey, witness);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        // Failture cases
        config.rpc_client = Some(RpcClient::new("fails".to_string()));

        config.command = WalletCommand::Airdrop(50);
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Balance;
        assert!(process_command(&config).is_err());

        let any_signature = Signature::new(&bs58::decode(SIGNATURE).into_vec().unwrap());
        config.command = WalletCommand::Confirm(any_signature);
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::ConfigureStakingAccount(None, Some(bob_pubkey));
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::CreateStakingAccount(bob_pubkey, 10);
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::GetTransactionCount;
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Pay(10, bob_pubkey, None, None, None, None);
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Pay(
            10,
            bob_pubkey,
            Some(dt),
            Some(config.id.pubkey()),
            None,
            None,
        );
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Pay(
            10,
            bob_pubkey,
            None,
            None,
            Some(vec![witness]),
            Some(config.id.pubkey()),
        );
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::TimeElapsed(bob_pubkey, process_id, dt);
        assert!(process_command(&config).is_err());
    }

    #[test]
    fn test_wallet_deploy() {
        solana_logger::setup();
        let mut pathbuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pathbuf.push("tests");
        pathbuf.push("fixtures");
        pathbuf.push("noop");
        pathbuf.set_extension("so");

        // Success case
        let mut config = WalletConfig::default();
        config.rpc_client = Some(RpcClient::new("succeeds".to_string()));

        config.command = WalletCommand::Deploy(pathbuf.to_str().unwrap().to_string());
        let result = process_command(&config);
        let json: Value = serde_json::from_str(&result.unwrap()).unwrap();
        let program_id = json
            .as_object()
            .unwrap()
            .get("programId")
            .unwrap()
            .as_str()
            .unwrap();
        let program_id_vec = bs58::decode(program_id).into_vec().unwrap();
        assert_eq!(program_id_vec.len(), mem::size_of::<Pubkey>());

        // Failure cases
        config.rpc_client = Some(RpcClient::new("airdrop".to_string()));
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Deploy("bad/file/location.so".to_string());
        assert!(process_command(&config).is_err());
    }

    fn tmp_file_path(name: &str) -> String {
        use std::env;
        let out_dir = env::var("OUT_DIR").unwrap_or_else(|_| "target".to_string());
        let keypair = Keypair::new();

        format!("{}/tmp/{}-{}", out_dir, name, keypair.pubkey()).to_string()
    }

    #[test]
    fn test_wallet_gen_keypair_file() {
        let outfile = tmp_file_path("test_gen_keypair_file.json");
        let serialized_keypair = gen_keypair_file(outfile.to_string()).unwrap();
        let keypair_vec: Vec<u8> = serde_json::from_str(&serialized_keypair).unwrap();
        assert!(Path::new(&outfile).exists());
        assert_eq!(keypair_vec, read_pkcs8(&outfile).unwrap());
        read_keypair(&outfile).unwrap();
        assert_eq!(
            read_keypair(&outfile).unwrap().pubkey().as_ref().len(),
            mem::size_of::<Pubkey>()
        );
        fs::remove_file(&outfile).unwrap();
        assert!(!Path::new(&outfile).exists());
    }

    #[test]
    fn test_wallet_get_recent_blockhash() {
        let rpc_client = RpcClient::new("succeeds".to_string());

        let vec = bs58::decode(PUBKEY).into_vec().unwrap();
        let expected_blockhash = Hash::new(&vec);

        let blockhash = get_recent_blockhash(&rpc_client);
        assert_eq!(blockhash.unwrap(), expected_blockhash);

        let rpc_client = RpcClient::new("fails".to_string());

        let blockhash = get_recent_blockhash(&rpc_client);
        assert!(blockhash.is_err());
    }

    #[test]
    fn test_wallet_send_transaction() {
        let rpc_client = RpcClient::new("succeeds".to_string());

        let key = Keypair::new();
        let to = Keypair::new().pubkey();
        let blockhash = Hash::default();
        let tx = SystemTransaction::new_account(&key, &to, 50, blockhash, 0);

        let signature = send_transaction(&rpc_client, &tx);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let rpc_client = RpcClient::new("fails".to_string());

        let signature = send_transaction(&rpc_client, &tx);
        assert!(signature.is_err());
    }

    #[test]
    fn test_wallet_confirm_transaction() {
        let rpc_client = RpcClient::new("succeeds".to_string());
        let signature = "good_signature";
        let status = confirm_transaction(&rpc_client, &signature);
        assert_eq!(status.unwrap(), RpcSignatureStatus::Confirmed);

        let rpc_client = RpcClient::new("bad_sig_status".to_string());
        let signature = "bad_status";
        let status = confirm_transaction(&rpc_client, &signature);
        assert!(status.is_err());

        let rpc_client = RpcClient::new("fails".to_string());
        let signature = "bad_status_fmt";
        let status = confirm_transaction(&rpc_client, &signature);
        assert!(status.is_err());
    }

    #[test]
    fn test_wallet_send_and_confirm_transaction() {
        let rpc_client = RpcClient::new("succeeds".to_string());

        let key = Keypair::new();
        let to = Keypair::new().pubkey();
        let blockhash = Hash::default();
        let mut tx = SystemTransaction::new_account(&key, &to, 50, blockhash, 0);

        let result = send_and_confirm_transaction(&rpc_client, &mut tx, &key);
        result.unwrap();

        let rpc_client = RpcClient::new("account_in_use".to_string());
        let result = send_and_confirm_transaction(&rpc_client, &mut tx, &key);
        assert!(result.is_err());

        let rpc_client = RpcClient::new("fails".to_string());
        let result = send_and_confirm_transaction(&rpc_client, &mut tx, &key);
        assert!(result.is_err());
    }

    #[test]
    fn test_wallet_resign_transaction() {
        let rpc_client = RpcClient::new("succeeds".to_string());

        let key = Keypair::new();
        let to = Keypair::new().pubkey();
        let vec = bs58::decode("HUu3LwEzGRsUkuJS121jzkPJW39Kq62pXCTmTa1F9jDL")
            .into_vec()
            .unwrap();
        let blockhash = Hash::new(&vec);
        let prev_tx = SystemTransaction::new_account(&key, &to, 50, blockhash, 0);
        let mut tx = SystemTransaction::new_account(&key, &to, 50, blockhash, 0);

        resign_transaction(&rpc_client, &mut tx, &key).unwrap();

        assert_ne!(prev_tx, tx);
        assert_ne!(prev_tx.signatures, tx.signatures);
        assert_ne!(prev_tx.recent_blockhash, tx.recent_blockhash);
        assert_eq!(prev_tx.fee, tx.fee);
        assert_eq!(prev_tx.account_keys, tx.account_keys);
        assert_eq!(prev_tx.instructions, tx.instructions);
    }

    #[test]
    fn test_request_and_confirm_airdrop() {
        let rpc_client = RpcClient::new("succeeds".to_string());
        let drone_addr = socketaddr!(0, 0);
        let pubkey = Keypair::new().pubkey();
        let lamports = 50;
        assert_eq!(
            request_and_confirm_airdrop(&rpc_client, &drone_addr, &pubkey, lamports).unwrap(),
            ()
        );

        let lamports = 0;
        assert!(request_and_confirm_airdrop(&rpc_client, &drone_addr, &pubkey, lamports).is_err());
    }
}
