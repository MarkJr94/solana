//! The `ledger_storage` module is intended to provide an abstraction for
//! parallel verification, iterative read, append-based writing, and random-access
//! reading of a persistent ledger

use crate::entry::Entry;
use crate::mint::Mint;
use crate::packet::{Blob, SharedBlob};

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;

use std::borrow::Borrow;

pub const DB_LEDGER_DIRECTORY: &str = "rocksdb";

#[derive(Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
// The Meta column family
pub struct SlotMeta {
    // The total number of consecutive blob starting from index 0
    // we have received for this slot.
    pub consumed: u64,
    // The entry height of the highest blob received for this slot.
    pub received: u64,
    // The slot the blob with index == "consumed" is in
    pub consumed_slot: u64,
    // The slot the blob with index == "received" is in
    pub received_slot: u64,
}

impl SlotMeta {
    pub(crate) fn new() -> Self {
        SlotMeta {
            consumed: 0,
            received: 0,
            consumed_slot: 0,
            received_slot: 0,
        }
    }
}

/// Intended be a replacement for direct use of anything from the `db_ledger` module
pub trait LedgerStorage: Sized {
    type Error;

    const DEFAULT_SLOT_HEIGHT: u64;

    fn open(ledger_path: &str) -> Result<Self, Self::Error>;

    fn meta(&self) -> Result<Option<SlotMeta>, Self::Error>;

    fn destroy(ledger_pat: &str) -> Result<(), Self::Error>;

    fn write_shared_blobs<I>(&self, shared_blobs: I) -> Result<Vec<Entry>, Self::Error>
    where
        I: IntoIterator,
        I::Item: Borrow<SharedBlob>;

    fn write_blobs<'a, I>(&self, blobs: I) -> Result<Vec<Entry>, Self::Error>
    where
        I: IntoIterator,
        I::Item: Borrow<&'a Blob>;

    fn write_entries<I>(
        &self,
        slot: u64,
        index: u64,
        entries: I,
    ) -> Result<Vec<Entry>, Self::Error>
    where
        I: IntoIterator,
        I::Item: Borrow<Entry>;

    fn insert_data_blobs<I>(&self, new_blobs: I) -> Result<Vec<Entry>, Self::Error>
    where
        I: IntoIterator,
        I::Item: Borrow<Blob>;

    /// Writes a list of sorted consecutive broadcast blobs to the ledger
    /// # Arguments
    /// * `blobs` - these must be consecutive and sorted on `(slot_index, blob_index)`
    fn write_consecutive_blobs(&self, blobs: &[SharedBlob]) -> Result<(), Self::Error>;

    ///Fill 'buf' with num_blobs or most number of consecutive
    /// whole blobs that fit into `buf.len()`
    ///
    /// Returns tuple of `(<number of blobs read>, <total size of blobs read>)`
    fn read_blobs_bytes(
        &self,
        start_index: u64,
        num_blobs: u64,
        buf: &mut [u8],
        slot_height: u64,
    ) -> Result<(u64, u64), Self::Error>;

    // /// Returns an iterator over all entries in the ledger
    // fn read_ledger<I>(&self) -> Result<I, Self::Error>
    // where
    //     I: IntoIterator<Item = Entry>;

    fn get_coding_blob_bytes(&self, slot: u64, index: u64) -> Result<Option<Vec<u8>>, Self::Error>;

    fn delete_coding_blob(&self, slot: u64, index: u64) -> Result<(), Self::Error>;

    fn get_data_blob_bytes(&self, slot: u64, index: u64) -> Result<Option<Vec<u8>>, Self::Error>;

    fn put_coding_blob_bytes(&self, slot: u64, index: u64, bytes: &[u8])
        -> Result<(), Self::Error>;

    fn put_data_blob_bytes(&self, slot: u64, index: u64, bytes: &[u8]) -> Result<(), Self::Error>;

    fn get_data_blob(&self, slot: u64, index: u64) -> Result<Option<Blob>, Self::Error>;

    /// Fill `buf` with `num_entries` or the most consecutive whole blobs
    /// that fit into `buf.len()`
    /// Returns tuple of `(<number of entries read>, <total size of entries read>)`
    fn get_entries_bytes(
        &self,
        start_index: u64,
        num_entries: u64,
        buf: &mut [u8],
    ) -> Result<(u64, u64), Self::Error>;
}

pub trait LedgerStorageExt: LedgerStorage {
    /// creates a ledger at the given path, signing the blobs with `keypair`.
    ///
    /// `entries` are used to generate the first blobs and create the genesis tick
    fn genesis<'a, I>(ledger_path: &str, keypair: &Keypair, entries: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = &'a Entry>;

    /// Given the ledger name, returns path to that ledger
    /// There is no guarantee that this ledger actually exists
    fn get_tmp_ledger_path(name: &str) -> String;

    /// Creates a temporary ledger with a genesis tick and returns the path to it
    fn create_tmp_ledger_with_mint(name: &str, mint: &Mint) -> String;

    /// Creates a temporary ledger with genesis tick and returns a the mint and path
    fn create_tmp_genesis(
        name: &str,
        num: u64,
        bootstrap_leader_id: Pubkey,
        bootstrap_leader_tokens: u64,
    ) -> (Mint, String);

    /// Creates a temporary ledger with genesis tick and sample ticks
    fn create_tmp_sample_ledger(
        name: &str,
        num_tokens: u64,
        num_ending_ticks: usize,
        bootstrap_leader_id: Pubkey,
        bootstrap_leader_tokens: u64,
    ) -> (Mint, String, Vec<Entry>);

    /// Creates a temporary ledger with the name `name` copied from the ledger  at `from`
    /// and returns its path
    fn tmp_copy_ledger(from: &str, name: &str) -> String;
}
