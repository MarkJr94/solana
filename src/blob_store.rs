//! The `blob_store` module is intended to export types that  implement the interfaces presented in
//! `ledger_storage` an possibly eventually replace `DbLedger`

#![allow(dead_code, unused_imports)]
use crate::db_ledger::SlotMeta;
use crate::entry::Entry;
use crate::ledger_storage::{LedgerStorage, LedgerStorageExt};
use crate::mint::Mint;
use crate::packet::{Blob, SharedBlob};

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;

use std::borrow::Borrow;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub struct BlobStore {
    root: PathBuf,
}

#[derive(Debug)]
pub enum BlobStoreError {
    Io(io::Error),
}
