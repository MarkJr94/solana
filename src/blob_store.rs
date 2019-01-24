//! The `blob_store` module is intended to export types that  implement the interfaces presented in
//! `ledger_storage` an possibly eventually replace `DbLedger`
//!

// TODO: Remove this once implementation is filed out
#![allow(unused_variables, dead_code, unused_imports)]

mod blob_store_impl;
#[cfg(test)]
mod tests;

use std::borrow::Borrow;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::result::Result as StdRes;
use std::sync::{Arc, RwLock};

use bincode::{deserialize, serialize};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, KeypairUtil};

use crate::entry::{create_ticks, Entry};
use crate::ledger_storage::{LedgerStorage, LedgerStorageExt, SlotMeta};
use crate::mint::Mint;
use crate::packet::{
    Blob, SharedBlob, BLOB_DATA_SIZE, BLOB_FLAG_IS_CODING, BLOB_HEADER_SIZE, BLOB_SIZE,
};

use self::blob_store_impl::*;

// local aliases to save typing
type Result<T> = StdRes<T, BlobStoreError>;

const DATA_FILE_NAME: &str = "data";
const META_FILE_NAME: &str = "meta";
const INDEX_FILE_NAME: &str = "index";
const ERASURE_FILE_NAME: &str = "index";
const INDEX_RECORD_SIZE: usize = 3 * 8; // 3 u64s

pub struct BlobStore {
    root: PathBuf,
}

#[derive(Debug)]
pub enum BlobStoreError {
    Io(io::Error),
    BadBlob,
    NoSuchBlob,
    Bincode(bincode::Error),
}

/// Dummy struct that will disappear, just getting tests compiling
pub struct Entries;

pub struct DataIter;

/// Dummy struct that will disappear, just getting tests compiling
impl BlobStore {
    pub fn new(root_path: &str) -> BlobStore {
        BlobStore {
            root: PathBuf::from(root_path),
        }
    }

    /// Return an iterator for all the entries in the given file.
    #[allow(unreachable_code)]
    pub fn read_ledger(&self) -> Result<impl Iterator<Item = Entry>> {
        unimplemented!();
        Ok(Entries)
    }
}

impl Iterator for Entries {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        unimplemented!()
    }
}

impl LedgerStorage for BlobStore {
    type Error = BlobStoreError;

    const DEFAULT_SLOT_HEIGHT: u64 = 0;

    fn open(ledger_path: &str) -> Result<Self> {
        Ok(BlobStore::new(ledger_path))
    }

    fn meta(&self) -> Result<Option<SlotMeta>> {
        self.get_meta(&meta_key(BlobStore::DEFAULT_SLOT_HEIGHT))
    }

    fn destroy(ledger_pat: &str) -> Result<()> {
        let path = Path::new(ledger_pat);
        // TODO: Mimicking old behavior probably get rid of it
        fs::create_dir_all(&path)?;
        fs::remove_dir_all(&path)?;
        Ok(())
    }

    fn write_shared_blobs<I>(&self, shared_blobs: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<SharedBlob>,
    {
        let c_blobs: Vec<_> = shared_blobs
            .into_iter()
            .map(move |s| s.borrow().clone())
            .collect();

        let r_blobs: Vec<_> = c_blobs.iter().map(move |b| b.read().unwrap()).collect();

        let blobs = r_blobs.iter().map(|s| &**s);

        let new_entries = self.insert_data_blobs(blobs)?;
        Ok(new_entries)
    }

    fn write_blobs<'a, I>(&self, blobs: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<&'a Blob>,
    {
        let blobs = blobs.into_iter().map(|b| *b.borrow());
        let new_entries = self.insert_data_blobs(blobs)?;
        Ok(new_entries)
    }

    fn write_entries<I>(&self, slot: u64, index: u64, entries: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<Entry>,
    {
        let blobs: Vec<_> = entries
            .into_iter()
            .enumerate()
            .map(|(idx, entry)| {
                let mut b = entry.borrow().to_blob();
                b.set_index(idx as u64 + index).map_err(bad_blob)?;
                b.set_slot(slot).map_err(bad_blob)?;
                Ok(b)
            })
            .collect::<Result<Vec<_>>>()?;

        self.write_blobs(&blobs)
    }

    // TODO: Accomplish Atomic writes of blobs + meta + offset
    fn insert_data_blobs<I>(&self, new_blobs: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<Blob>,
    {
        let mut new_blobs: Vec<_> = new_blobs.into_iter().collect();

        if new_blobs.is_empty() {
            return Ok(vec![]);
        }

        // sort on (slot_idx, idx) tuple
        new_blobs.sort_unstable_by(|b1, b2| {
            let b1 = b1.borrow();
            let b1_key = (b1.slot().unwrap(), b1.index().unwrap());
            let b2 = b2.borrow();
            let b2_key = (b2.slot().unwrap(), b2.index().unwrap());
            b1_key.cmp(&b2_key)
        });

        // contains the indices into new_blobs of the first blob for that slot
        let mut slot_starts = vec![];
        let mut max_slot = new_blobs[0].borrow().slot().map_err(bad_blob)?;
        slot_starts.push((0, max_slot));
        for (index, slot) in new_blobs
            .iter()
            .map(|b| b.borrow().slot().unwrap())
            .enumerate()
        {
            if slot > max_slot {
                max_slot = slot;
                slot_starts.push((index, max_slot));
            }
        }

        let slot_ranges = slot_starts
            .windows(2)
            .map(|window| (window[0].0..window[1].0, window[0].1))
            .collect::<Vec<_>>();

        let mut entries: Vec<Entry> = Vec::new();

        for (range, slot) in slot_ranges {
            let slot_path = self.mk_slot_path(slot);

            // load slot meta
            let meta_path = slot_path.join(META_FILE_NAME);

            let (mut meta_file, old_slot_meta) = if meta_path.exists() {
                let mut f = OpenOptions::new().read(true).write(true).open(meta_path)?;
                let m: SlotMeta = bincode::deserialize_from(&f)?;
                // truncate and reset cursor
                f.set_len(0)?;
                f.seek(SeekFrom::Start(0))?;
                (f, m)
            } else {
                let f = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(meta_path)?;
                (f, SlotMeta::new())
            };

            let mut new_slot_meta = old_slot_meta;

            // write blobs
            let data_path = slot_path.join(DATA_FILE_NAME);

            let mut data_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&data_path)?;

            let slot_blobs = &new_blobs[range];

            let mut index_info = vec![];

            // TODO: this should be a single write call
            for blob in slot_blobs {
                let blob = blob.borrow();
                let (blob_slot, blob_index) = (
                    blob.slot().map_err(bad_blob)?,
                    blob.index().map_err(bad_blob)?,
                );
                let serialized_blob_datas =
                    &blob.data[..BLOB_HEADER_SIZE + blob.size().map_err(bad_blob)?];
                let serialized_entry_data = &blob.data
                    [BLOB_HEADER_SIZE..BLOB_HEADER_SIZE + blob.size().map_err(bad_blob)?];

                let entry = deserialize(serialized_entry_data)
                    .expect("Blob passed validation and must be deserializable");
                entries.push(entry);

                let key = data_key(blob_slot, blob_index);

                let blob_offset = data_file.seek(SeekFrom::Current(0))?;

                let serialized_len = serialized_blob_datas.len();
                data_file.write_all(serialized_blob_datas)?;

                index_info.push((blob_index, blob_offset, serialized_len));

                // Do stuff for received and consumed
                if blob_index >= new_slot_meta.received {
                    new_slot_meta.received = blob_index + 1;
                }

                if blob_index == new_slot_meta.consumed + 1 {
                    new_slot_meta.consumed += 1;
                }
            }

            // Write slot meta
            meta_file.write_all(&mut serialize(&new_slot_meta)?)?;

            // Write index stuff
            let index_path = slot_path.join(INDEX_FILE_NAME);
            let mut index_file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&index_path)?;

            // TODO: Bundle writes
            for (index, offset, len) in index_info {
                let mut rec = [0u8; INDEX_RECORD_SIZE];
                BigEndian::write_u64(&mut rec[0..8], index);
                BigEndian::write_u64(&mut rec[8..16], offset);
                BigEndian::write_u64(&mut rec[16..24], len as u64);

                index_file.write_all(&mut rec)?;
            }

            index_file.sync_data()?;
            meta_file.sync_data()?;
            data_file.sync_data()?;
        }

        // // TODO: copied from db_ledger must address
        // // TODO: Handle if leader sends different blob for same index when the index > consumed
        // // The old window implementation would just replace that index.
        Ok(entries)
    }

    fn write_consecutive_blobs(&self, blobs: &[Arc<RwLock<Blob>>]) -> Result<()> {
        self.write_shared_blobs(blobs)?;
        Ok(())
    }

    fn read_blobs_bytes(
        &self,
        start_index: u64,
        num_blobs: u64,
        buf: &mut [u8],
        slot_height: u64,
    ) -> Result<(u64, u64)> {
        unimplemented!()
        // start a start slot.
    }

    fn get_coding_blob_bytes(&self, slot: u64, index: u64) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn delete_coding_blob(&self, slot: u64, index: u64) -> Result<()> {
        unimplemented!()
    }

    fn get_data_blob_bytes(&self, slot: u64, index: u64) -> Result<Option<Vec<u8>>> {
        self.get_data(&data_key(slot, index))
    }

    fn put_coding_blob_bytes(&self, slot: u64, index: u64, bytes: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn put_data_blob_bytes(&self, slot: u64, index: u64, bytes: &[u8]) -> Result<()> {
        self.put_data(&data_key(slot, index), bytes)
    }

    fn get_data_blob(&self, slot: u64, index: u64) -> Result<Option<Blob>> {
        let bytes = self.get_data_blob_bytes(slot, index)?;
        Ok(bytes.map(|bytes| {
            let blob = Blob::new(&bytes);
            assert!(blob.slot().unwrap() == slot);
            assert!(blob.index().unwrap() == index);
            blob
        }))
    }

    fn get_entries_bytes(
        &self,
        start_index: u64,
        num_entries: u64,
        buf: &mut [u8],
    ) -> Result<(u64, u64)> {
        unimplemented!()
    }
}

impl LedgerStorageExt for BlobStore {
    fn genesis<'a, I>(ledger_path: &str, keypair: &Keypair, entries: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a Entry>,
    {
        let blob_store = BlobStore::open(ledger_path)?;

        // TODO: sign blobs
        let blobs: Vec<_> = entries
            .into_iter()
            .enumerate()
            .map(|(idx, entry)| {
                let mut b = entry.borrow().to_blob();
                b.set_index(idx as u64).unwrap();
                b.set_id(&keypair.pubkey()).unwrap();
                b.set_slot(BlobStore::DEFAULT_SLOT_HEIGHT).unwrap();
                b
            })
            .collect();

        blob_store.write_blobs(&blobs[..])?;
        Ok(())
    }

    fn get_tmp_ledger_path(name: &str) -> String {
        use std::env;
        let out_dir = env::var("OUT_DIR").unwrap_or_else(|_| "target".to_string());
        let keypair = Keypair::new();

        let path = format!("{}/tmp/ledger-{}-{}", out_dir, name, keypair.pubkey());

        // whack any possible collision
        let _ignored = fs::remove_dir_all(&path);

        path
    }

    fn create_tmp_ledger_with_mint(name: &str, mint: &Mint) -> String {
        let path = Self::get_tmp_ledger_path(name);
        BlobStore::destroy(&path).expect("Expected successful blob store destruction");
        let db_ledger = BlobStore::open(&path).unwrap();
        db_ledger
            .write_entries(Self::DEFAULT_SLOT_HEIGHT, 0, &mint.create_entries())
            .unwrap();

        path
    }

    fn create_tmp_genesis(
        name: &str,
        num: u64,
        bootstrap_leader_id: Pubkey,
        bootstrap_leader_tokens: u64,
    ) -> (Mint, String) {
        let mint = Mint::new_with_leader(num, bootstrap_leader_id, bootstrap_leader_tokens);
        let path = Self::create_tmp_ledger_with_mint(name, &mint);

        (mint, path)
    }

    fn create_tmp_sample_ledger(
        name: &str,
        num_tokens: u64,
        num_ending_ticks: usize,
        bootstrap_leader_id: Pubkey,
        bootstrap_leader_tokens: u64,
    ) -> (Mint, String, Vec<Entry>) {
        let mint = Mint::new_with_leader(num_tokens, bootstrap_leader_id, bootstrap_leader_tokens);
        let path = Self::get_tmp_ledger_path(name);

        // Create the entries
        let mut genesis = mint.create_entries();
        let ticks = create_ticks(num_ending_ticks, mint.last_id());
        genesis.extend(ticks);

        BlobStore::destroy(&path).expect("Expected successful database destruction");
        let db_ledger = BlobStore::open(&path).unwrap();
        db_ledger
            .write_entries(BlobStore::DEFAULT_SLOT_HEIGHT, 0, &genesis)
            .unwrap();

        (mint, path, genesis)
    }

    fn tmp_copy_ledger(from: &str, name: &str) -> String {
        let tostr = Self::get_tmp_ledger_path(name);

        let blob_store = BlobStore::open(from).unwrap();
        let ledger_entries = blob_store.read_ledger().unwrap();

        BlobStore::destroy(&tostr).expect("Expected successful database destruction");
        let blob_store = BlobStore::open(&tostr).unwrap();
        blob_store
            .write_entries(BlobStore::DEFAULT_SLOT_HEIGHT, 0, ledger_entries)
            .unwrap();

        tostr
    }
}

impl fmt::Display for BlobStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BlobStoreError::Io(_) => write!(
                f,
                "Blob-Store Error: I/O. The storage folder may be corrupted"
            ),
            BlobStoreError::BadBlob => write!(
                f,
                "Blob-Store Error: Malformed Blob: The Store may be corrupted"
            ),
            BlobStoreError::NoSuchBlob => write!(
                f,
                "Blob-Store Error: Invalid Blob Index. No such blob exists."
            ),
            BlobStoreError::Bincode(_) => {
                write!(f, "Blob-Store Error: internal application error.")
            }
        }
    }
}

impl Error for BlobStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BlobStoreError::Io(e) => Some(e),
            BlobStoreError::Bincode(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for BlobStoreError {
    fn from(e: io::Error) -> BlobStoreError {
        BlobStoreError::Io(e)
    }
}

impl From<bincode::Error> for BlobStoreError {
    fn from(e: bincode::Error) -> BlobStoreError {
        BlobStoreError::Bincode(e)
    }
}

impl Iterator for DataIter {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

impl DataIter {
    fn seek(&self, key: &[u8]) {}

    fn valid(&self) -> bool {
        unimplemented!()
    }

    fn key(&self) -> Result<Vec<u8>> {
        unimplemented!()
    }
}
