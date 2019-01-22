//! The `blob_store` module is intended to export types that  implement the interfaces presented in
//! `ledger_storage` an possibly eventually replace `DbLedger`

// TODO: Remove this once implementation is filed out
#![allow(unused_variables, dead_code, unused_imports)]

use std::borrow::Borrow;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::result::Result as StdRes;
use std::sync::{Arc, RwLock};

use bincode::{deserialize, serialize};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, KeypairUtil};

use crate::entry::Entry;
use crate::ledger_storage::{LedgerStorage, LedgerStorageExt, SlotMeta};
use crate::mint::Mint;
use crate::packet::{Blob, SharedBlob};

// local aliases to save typing
type Result<T> = StdRes<T, BlobStoreError>;

pub struct BlobStore {
    root: PathBuf,
}

#[derive(Debug)]
pub enum BlobStoreError {
    Io(io::Error),
}

impl From<io::Error> for BlobStoreError {
    fn from(e: io::Error) -> BlobStoreError {
        BlobStoreError::Io(e)
    }
}

/// Dummy struct that will disappear, just getting tests compiling
pub struct DataIter;

impl DataIter {
    fn seek(&mut self, key: &[u8]) {
        unimplemented!()
    }

    fn valid(&self) -> bool {
        unimplemented!()
    }

    fn key(&self) -> Option<Vec<u8>> {
        unimplemented!()
    }

    fn next(&mut self) {
        unimplemented!()
    }
}

/// Dummy struct that will disappear, just getting tests compiling
pub struct Entries;

impl Iterator for Entries {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        unimplemented!()
    }
}

impl BlobStore {
    pub fn new(root_path: &str) -> BlobStore {
        BlobStore {
            root: PathBuf::from(root_path),
        }
    }

    /// Dummy function that will disappear, just getting tests compiling
    fn get_meta(&self, key: &[u8]) -> Result<Option<SlotMeta>> {
        unimplemented!()
    }

    /// Dummy function that will disappear, just getting tests compiling
    fn put_meta(&self, key: &[u8], meta: &SlotMeta) -> Result<()> {
        unimplemented!()
    }

    /// Dummy function that will disappear, just getting tests compiling
    fn get_data(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    /// Dummy function that will disappear, just getting tests compiling
    fn put_data(&self, key: &[u8], data: &[u8]) -> Result<()> {
        unimplemented!()
    }

    /// Dummy function that will disappear, just getting tests compiling
    fn get_erasure(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    /// Dummy function that will disappear, just getting tests compiling
    fn put_erasure(&self, key: &[u8], erasure: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn slot_height_from_key(key: &[u8]) -> Result<u64> {
        let mut rdr = io::Cursor::new(&key[0..8]);
        let height = rdr.read_u64::<BigEndian>()?;
        Ok(height)
    }

    fn data_iterator(&self) -> Result<DataIter> {
        unimplemented!()
    }

    #[allow(unreachable_code)]
    fn read_ledger(&self) -> Result<impl Iterator<Item = Entry>> {
        unimplemented!();
        Ok(Entries)
    }
}

impl LedgerStorage for BlobStore {
    type Error = BlobStoreError;

    const DEFAULT_SLOT_HEIGHT: u64 = 0;

    fn open(ledger_path: &str) -> Result<Self> {
        Ok(BlobStore::new(ledger_path))
    }

    fn meta(&self) -> Result<Option<SlotMeta>> {
        unimplemented!()
    }

    fn destroy(ledger_pat: &str) -> Result<()> {
        unimplemented!()
    }

    fn write_shared_blobs<I>(&self, shared_blobs: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<SharedBlob>,
    {
        unimplemented!()
    }

    fn write_blobs<'a, I>(&self, blobs: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<&'a Blob>,
    {
        unimplemented!()
    }

    fn write_entries<I>(&self, slot: u64, index: u64, entries: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<Entry>,
    {
        unimplemented!()
    }

    fn insert_data_blobs<I>(&self, new_blobs: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<Blob>,
    {
        unimplemented!()
    }

    fn write_consecutive_blobs(&self, blobs: &[Arc<RwLock<Blob>>]) -> Result<()> {
        unimplemented!()
    }

    fn read_blobs_bytes(
        &self,
        start_index: u64,
        num_blobs: u64,
        buf: &mut [u8],
        slot_height: u64,
    ) -> Result<(u64, u64)> {
        unimplemented!()
    }

    fn get_coding_blob_bytes(&self, slot: u64, index: u64) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn delete_coding_blob(&self, slot: u64, index: u64) -> Result<()> {
        unimplemented!()
    }

    fn get_data_blob_bytes(&self, slot: u64, index: u64) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn put_coding_blob_bytes(&self, slot: u64, index: u64, bytes: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn put_data_blob_bytes(&self, slot: u64, index: u64, bytes: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn get_data_blob(&self, slot: u64, index: u64) -> Result<Option<Blob>> {
        unimplemented!()
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
    fn genesis<'a, I>(ledger_path: &str, keypair: &Keypair, entries: I) -> Result<()> {
        unimplemented!()
    }

    fn get_tmp_ledger_path(name: &str) -> String {
        unimplemented!()
    }

    fn create_tmp_ledger_with_mint(name: &str, mint: &Mint) -> String {
        unimplemented!()
    }

    fn create_tmp_genesis(
        name: &str,
        num: u64,
        bootstrap_leader_id: Pubkey,
        bootstrap_leader_tokens: u64,
    ) -> (Mint, String) {
        unimplemented!()
    }

    fn create_tmp_sample_ledger(
        name: &str,
        num_tokens: u64,
        num_ending_ticks: usize,
        bootstrap_leader_id: Pubkey,
        bootstrap_leader_tokens: u64,
    ) -> (Mint, String, Vec<Entry>) {
        unimplemented!()
    }

    fn tmp_copy_ledger(from: &str, name: &str) -> String {
        unimplemented!()
    }
}

fn meta_key(slot_height: u64) -> Vec<u8> {
    let mut key = vec![0u8; 8];
    BigEndian::write_u64(&mut key[0..8], slot_height);
    key
}

fn data_key(slot_height: u64, index: u64) -> Vec<u8> {
    let mut key = vec![0u8; 16];
    BigEndian::write_u64(&mut key[0..8], slot_height);
    BigEndian::write_u64(&mut key[8..16], index);
    key
}

fn data_index_from_key(key: &[u8]) -> Result<u64> {
    let mut rdr = io::Cursor::new(&key[8..16]);
    let index = rdr.read_u64::<BigEndian>()?;
    Ok(index)
}

fn erasure_key(slot_height: u64, index: u64) -> Vec<u8> {
    data_key(slot_height, index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_ledger::DEFAULT_SLOT_HEIGHT;
    use crate::entry::{make_tiny_test_entries, EntrySlice};
    use crate::packet::index_blobs;

    #[test]
    fn test_put_get_simple() {
        let ledger_path = BlobStore::get_tmp_ledger_path("test_put_get_simple");
        let ledger = BlobStore::open(&ledger_path).unwrap();

        // Test meta column family
        let meta = SlotMeta::new();
        let meta_key = meta_key(DEFAULT_SLOT_HEIGHT);
        ledger.put_meta(&meta_key, &meta).unwrap();
        let result = ledger
            .get_meta(&meta_key)
            .unwrap()
            .expect("Expected meta object to exist");

        assert_eq!(result, meta);

        // Test erasure column family
        let erasure = vec![1u8; 16];
        let erasure_key = erasure_key(DEFAULT_SLOT_HEIGHT, 0);
        ledger.put_erasure(&erasure_key, &erasure).unwrap();

        let result = ledger
            .get_erasure(&erasure_key)
            .unwrap()
            .expect("Expected erasure object to exist");

        assert_eq!(result, erasure);

        // Test data column family
        let data = vec![2u8; 16];
        let data_key = data_key(DEFAULT_SLOT_HEIGHT, 0);
        ledger.put_data(&data_key, &data).unwrap();

        let result = ledger
            .get_data(&data_key)
            .unwrap()
            .expect("Expected data object to exist");

        assert_eq!(result, data);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlobStore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_read_blobs_bytes() {
        let shared_blobs = make_tiny_test_entries(10).to_shared_blobs();
        let slot = DEFAULT_SLOT_HEIGHT;
        index_blobs(&shared_blobs, &Keypair::new().pubkey(), 0, &[slot; 10]);

        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();

        let ledger_path = BlobStore::get_tmp_ledger_path("test_read_blobs_bytes");
        let ledger = BlobStore::open(&ledger_path).unwrap();
        ledger.write_blobs(&blobs).unwrap();

        let mut buf = [0; 1024];
        let (num_blobs, bytes) = ledger.read_blobs_bytes(0, 1, &mut buf, slot).unwrap();
        let bytes = bytes as usize;
        assert_eq!(num_blobs, 1);
        {
            let blob_data = &buf[..bytes];
            assert_eq!(blob_data, &blobs[0].data[..bytes]);
        }

        let (num_blobs, bytes2) = ledger.read_blobs_bytes(0, 2, &mut buf, slot).unwrap();
        let bytes2 = bytes2 as usize;
        assert_eq!(num_blobs, 2);
        assert!(bytes2 > bytes);
        {
            let blob_data_1 = &buf[..bytes];
            assert_eq!(blob_data_1, &blobs[0].data[..bytes]);

            let blob_data_2 = &buf[bytes..bytes2];
            assert_eq!(blob_data_2, &blobs[1].data[..bytes2 - bytes]);
        }

        // buf size part-way into blob[1], should just return blob[0]
        let mut buf = vec![0; bytes + 1];
        let (num_blobs, bytes3) = ledger.read_blobs_bytes(0, 2, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes3 = bytes3 as usize;
        assert_eq!(bytes3, bytes);

        let mut buf = vec![0; bytes2 - 1];
        let (num_blobs, bytes4) = ledger.read_blobs_bytes(0, 2, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes4 = bytes4 as usize;
        assert_eq!(bytes4, bytes);

        let mut buf = vec![0; bytes * 2];
        let (num_blobs, bytes6) = ledger.read_blobs_bytes(9, 1, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes6 = bytes6 as usize;

        {
            let blob_data = &buf[..bytes6];
            assert_eq!(blob_data, &blobs[9].data[..bytes6]);
        }

        // Read out of range
        assert!(ledger.read_blobs_bytes(20, 2, &mut buf, slot).is_err());

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlobStore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_blobs_basic() {
        let entries = make_tiny_test_entries(2);
        let shared_blobs = entries.to_shared_blobs();

        for (i, b) in shared_blobs.iter().enumerate() {
            b.write().unwrap().set_index(i as u64).unwrap();
        }

        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();

        let ledger_path = BlobStore::get_tmp_ledger_path("test_insert_data_blobs_basic");
        let ledger = BlobStore::open(&ledger_path).unwrap();

        // Insert second blob, we're missing the first blob, so should return nothing
        let result = ledger.insert_data_blobs(vec![blobs[1]]).unwrap();

        assert!(result.len() == 0);
        let meta = ledger
            .get_meta(&meta_key(DEFAULT_SLOT_HEIGHT))
            .unwrap()
            .expect("Expected new metadata object to be created");
        assert!(meta.consumed == 0 && meta.received == 2);

        // Insert first blob, check for consecutive returned entries
        let result = ledger.insert_data_blobs(vec![blobs[0]]).unwrap();

        assert_eq!(result, entries);

        let meta = ledger
            .get_meta(&meta_key(DEFAULT_SLOT_HEIGHT))
            .unwrap()
            .expect("Expected new metadata object to exist");
        assert!(meta.consumed == 2 && meta.received == 2);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlobStore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_blobs_multiple() {
        let num_blobs = 10;
        let entries = make_tiny_test_entries(num_blobs);
        let shared_blobs = entries.to_shared_blobs();
        for (i, b) in shared_blobs.iter().enumerate() {
            b.write().unwrap().set_index(i as u64).unwrap();
        }
        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();

        let ledger_path = BlobStore::get_tmp_ledger_path("test_insert_data_blobs_multiple");
        let ledger = BlobStore::open(&ledger_path).unwrap();

        // Insert blobs in reverse, check for consecutive returned blobs
        for i in (0..num_blobs).rev() {
            let result = ledger.insert_data_blobs(vec![blobs[i]]).unwrap();

            let meta = ledger
                .get_meta(&meta_key(DEFAULT_SLOT_HEIGHT))
                .unwrap()
                .expect("Expected metadata object to exist");
            if i != 0 {
                assert_eq!(result.len(), 0);
                assert!(meta.consumed == 0 && meta.received == num_blobs as u64);
            } else {
                assert_eq!(result, entries);
                assert!(meta.consumed == num_blobs as u64 && meta.received == num_blobs as u64);
            }
        }

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlobStore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_blobs_slots() {
        let num_blobs = 10;
        let entries = make_tiny_test_entries(num_blobs);
        let shared_blobs = entries.to_shared_blobs();
        for (i, b) in shared_blobs.iter().enumerate() {
            b.write().unwrap().set_index(i as u64).unwrap();
        }
        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();

        let ledger_path = BlobStore::get_tmp_ledger_path("test_insert_data_blobs_slots");
        let ledger = BlobStore::open(&ledger_path).unwrap();

        // Insert last blob into next slot
        let result = ledger
            .insert_data_blobs(vec![*blobs.last().unwrap()])
            .unwrap();
        assert_eq!(result.len(), 0);

        // Insert blobs into first slot, check for consecutive blobs
        for i in (0..num_blobs - 1).rev() {
            let result = ledger.insert_data_blobs(vec![blobs[i]]).unwrap();
            let meta = ledger
                .get_meta(&meta_key(DEFAULT_SLOT_HEIGHT))
                .unwrap()
                .expect("Expected metadata object to exist");
            if i != 0 {
                assert_eq!(result.len(), 0);
                assert!(meta.consumed == 0 && meta.received == num_blobs as u64);
            } else {
                assert_eq!(result, entries);
                assert!(meta.consumed == num_blobs as u64 && meta.received == num_blobs as u64);
            }
        }

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        BlobStore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_iteration_order() {
        let slot = 0;
        let db_ledger_path = BlobStore::get_tmp_ledger_path("test_iteration_order");
        {
            let db_ledger = BlobStore::open(&db_ledger_path).unwrap();

            // Write entries
            let num_entries = 8;
            let shared_blobs = make_tiny_test_entries(num_entries).to_shared_blobs();

            for (i, b) in shared_blobs.iter().enumerate() {
                let mut w_b = b.write().unwrap();
                w_b.set_index(1 << (i * 8)).unwrap();
                w_b.set_slot(DEFAULT_SLOT_HEIGHT).unwrap();
            }

            assert_eq!(
                db_ledger
                    .write_shared_blobs(&shared_blobs)
                    .expect("Expected successful write of blobs"),
                vec![]
            );
            let mut db_iterator = db_ledger
                .data_iterator()
                .expect("Expected to be able to open database iterator");

            db_iterator.seek(&data_key(slot, 1));

            // Iterate through ledger
            for i in 0..num_entries {
                assert!(db_iterator.valid());
                let current_key = db_iterator.key().expect("Expected a valid key");
                let current_index = data_index_from_key(&current_key)
                    .expect("Expect to be able to parse index from valid key");
                assert_eq!(current_index, (1 as u64) << (i * 8));
                db_iterator.next();
            }
        }
        BlobStore::destroy(&db_ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_insert_data_blobs_bulk() {
        let db_ledger_path = BlobStore::get_tmp_ledger_path("test_insert_data_blobs_bulk");
        {
            let db_ledger = BlobStore::open(&db_ledger_path).unwrap();

            // Write entries
            let num_entries = 20 as u64;
            let original_entries = make_tiny_test_entries(num_entries as usize);
            let shared_blobs = original_entries.clone().to_shared_blobs();
            for (i, b) in shared_blobs.iter().enumerate() {
                let mut w_b = b.write().unwrap();
                w_b.set_index(i as u64).unwrap();
                w_b.set_slot(i as u64).unwrap();
            }

            assert_eq!(
                db_ledger
                    .write_shared_blobs(shared_blobs.iter().skip(1).step_by(2))
                    .unwrap(),
                vec![]
            );

            assert_eq!(
                db_ledger
                    .write_shared_blobs(shared_blobs.iter().step_by(2))
                    .unwrap(),
                original_entries
            );

            let meta_key = meta_key(DEFAULT_SLOT_HEIGHT);
            let meta = db_ledger.get_meta(&meta_key).unwrap().unwrap();
            assert_eq!(meta.consumed, num_entries);
            assert_eq!(meta.received, num_entries);
            assert_eq!(meta.consumed_slot, num_entries - 1);
            assert_eq!(meta.received_slot, num_entries - 1);
        }
        BlobStore::destroy(&db_ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_insert_data_blobs_duplicate() {
        // Create RocksDb ledger
        let db_ledger_path = BlobStore::get_tmp_ledger_path("test_insert_data_blobs_duplicate");
        {
            let db_ledger = BlobStore::open(&db_ledger_path).unwrap();

            // Write entries
            let num_entries = 10 as u64;
            let num_duplicates = 2;
            let original_entries: Vec<Entry> = make_tiny_test_entries(num_entries as usize)
                .into_iter()
                .flat_map(|e| vec![e; num_duplicates])
                .collect();

            let shared_blobs = original_entries.clone().to_shared_blobs();
            for (i, b) in shared_blobs.iter().enumerate() {
                let index = (i / 2) as u64;
                let mut w_b = b.write().unwrap();
                w_b.set_index(index).unwrap();
                w_b.set_slot(index).unwrap();
            }

            assert_eq!(
                db_ledger
                    .write_shared_blobs(
                        shared_blobs
                            .iter()
                            .skip(num_duplicates)
                            .step_by(num_duplicates * 2)
                    )
                    .unwrap(),
                vec![]
            );

            let expected: Vec<_> = original_entries
                .into_iter()
                .step_by(num_duplicates)
                .collect();

            assert_eq!(
                db_ledger
                    .write_shared_blobs(shared_blobs.iter().step_by(num_duplicates * 2))
                    .unwrap(),
                expected,
            );

            let meta_key = meta_key(DEFAULT_SLOT_HEIGHT);
            let meta = db_ledger.get_meta(&meta_key).unwrap().unwrap();
            assert_eq!(meta.consumed, num_entries);
            assert_eq!(meta.received, num_entries);
            assert_eq!(meta.consumed_slot, num_entries - 1);
            assert_eq!(meta.received_slot, num_entries - 1);
        }
        BlobStore::destroy(&db_ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_write_consecutive_blobs() {
        let db_ledger_path = BlobStore::get_tmp_ledger_path("test_write_consecutive_blobs");
        {
            let db_ledger = BlobStore::open(&db_ledger_path).unwrap();

            // Write entries
            let num_entries = 20 as u64;
            let original_entries = make_tiny_test_entries(num_entries as usize);
            let shared_blobs = original_entries.to_shared_blobs();
            for (i, b) in shared_blobs.iter().enumerate() {
                let mut w_b = b.write().unwrap();
                w_b.set_index(i as u64).unwrap();
                w_b.set_slot(i as u64).unwrap();
            }

            db_ledger
                .write_consecutive_blobs(&shared_blobs)
                .expect("Expect successful blob writes");

            let meta_key = meta_key(DEFAULT_SLOT_HEIGHT);
            let meta = db_ledger.get_meta(&meta_key).unwrap().unwrap();
            assert_eq!(meta.consumed, num_entries);
            assert_eq!(meta.received, num_entries);
            assert_eq!(meta.consumed_slot, num_entries - 1);
            assert_eq!(meta.received_slot, num_entries - 1);

            for (i, b) in shared_blobs.iter().enumerate() {
                let mut w_b = b.write().unwrap();
                w_b.set_index(num_entries + i as u64).unwrap();
                w_b.set_slot(num_entries + i as u64).unwrap();
            }

            db_ledger
                .write_consecutive_blobs(&shared_blobs)
                .expect("Expect successful blob writes");

            let meta = db_ledger.get_meta(&meta_key).unwrap().unwrap();
            assert_eq!(meta.consumed, 2 * num_entries);
            assert_eq!(meta.received, 2 * num_entries);
            assert_eq!(meta.consumed_slot, 2 * num_entries - 1);
            assert_eq!(meta.received_slot, 2 * num_entries - 1);
        }
        BlobStore::destroy(&db_ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_genesis_and_entry_iterator() {
        let entries = make_tiny_test_entries(100);
        let ledger_path = BlobStore::get_tmp_ledger_path("test_genesis_and_entry_iterator");
        {
            assert!(BlobStore::genesis(&ledger_path, &Keypair::new(), &entries).is_ok());

            let ledger = BlobStore::open(&ledger_path).expect("open failed");

            let read_entries: Vec<Entry> =
                ledger.read_ledger().expect("read_ledger failed").collect();
            assert_eq!(entries, read_entries);
        }

        BlobStore::destroy(&ledger_path).expect("Expected successful database destruction");
    }
}
