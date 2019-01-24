use std::borrow::Borrow;
use std::collections::{self, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader, Seek, SeekFrom};
use std::marker::PhantomData;
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
use crate::packet::{
    Blob, SharedBlob, BLOB_DATA_SIZE, BLOB_FLAG_IS_CODING, BLOB_HEADER_SIZE, BLOB_SIZE,
};

use super::*;

impl BlobStore {
    /// Dummy function that will disappear, just getting tests compiling
    pub(super) fn get_meta(&self, key: &[u8]) -> Result<Option<SlotMeta>> {
        let slot_height = BlobStore::slot_height_from_key(key)?;
        let path = self.mk_slot_path(slot_height);

        let mut meta_file = File::open(path)?;
        let meta = bincode::deserialize_from(&mut meta_file)?;

        Ok(Some(meta))
    }

    /// Dummy function that will disappear, just getting tests compiling
    pub(super) fn put_meta(&self, key: &[u8], meta: &SlotMeta) -> Result<()> {
        let slot_height = BlobStore::slot_height_from_key(key)?;
        let mut meta_file = OpenOptions::new()
            .truncate(true)
            .create(true)
            .open(&self.mk_slot_path(slot_height))?;
        meta_file.write_all(&mut serialize(meta)?)?;
        Ok(())
    }

    /// Dummy function that will disappear, just getting tests compiling
    pub(super) fn get_data(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (slot_height, index) = BlobStore::indices_from_key(key)?;
        let (data_path, blob_loc) = match self.index_data(slot_height, index)? {
            Some(x) => x,
            None => {
                return Ok(None);
            }
        };

        let mut data_rdr = BufReader::new(File::open(&self.mk_data_path(slot_height))?);

        let mut buf = vec![0; blob_loc.size as usize];
        data_rdr.seek(SeekFrom::Start(blob_loc.offset))?;
        data_rdr.read_exact(&mut buf)?;
        Ok(Some(buf))
    }

    /// Dummy function that will disappear, just getting tests compiling
    pub(super) fn put_data(&self, key: &[u8], data: &[u8]) -> Result<()> {
        let (slot_height, index) = BlobStore::indices_from_key(key)?;
        let meta_path = self.mk_meta_path(slot_height);
        let index_path = self.mk_index_path(slot_height);
        let data_path = self.mk_data_path(slot_height);

        let mut data_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&data_path)?;

        let mut index_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)?;

        let offset = data_file.seek(SeekFrom::Current(0))?;

        let size = data.len();
        data_file.write_all(data)?;

        let mut rec = [0u8; INDEX_RECORD_SIZE];
        BigEndian::write_u64(&mut rec[0..8], index);
        BigEndian::write_u64(&mut rec[8..16], offset);
        BigEndian::write_u64(&mut rec[16..24], size as u64);

        index_file.write_all(&mut rec)?;
        Ok(())
    }

    /// Dummy function that will disappear, just getting tests compiling
    pub(super) fn get_erasure(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    /// Dummy function that will disappear, just getting tests compiling
    pub(super) fn put_erasure(&self, key: &[u8], erasure: &[u8]) -> Result<()> {
        unimplemented!()
    }

    pub(super) fn slot_height_from_key(key: &[u8]) -> Result<u64> {
        let mut rdr = io::Cursor::new(&key[0..8]);
        let height = rdr.read_u64::<BigEndian>()?;
        Ok(height)
    }

    pub(super) fn indices_from_key(key: &[u8]) -> Result<(u64, u64)> {
        let mut rdr = io::Cursor::new(&key[0..8]);
        let height = rdr.read_u64::<BigEndian>()?;
        let index = rdr.read_u64::<BigEndian>()?;
        Ok((height, index))
    }

    pub(super) fn blob_index_from_key(key: &[u8]) -> Result<u64> {
        let mut rdr = io::Cursor::new(&key[8..16]);
        let height = rdr.read_u64::<BigEndian>()?;
        Ok(height)
    }

    pub(super) fn data_iterator(&self) -> Result<DataIter> {
        unimplemented!()
    }

    pub(super) fn mk_slot_path(&self, slot_height: u64) -> PathBuf {
        self.root.join(&format!("{:#x}", slot_height))
    }

    pub(super) fn mk_data_path(&self, slot_height: u64) -> PathBuf {
        self.mk_slot_path(slot_height).join(DATA_FILE_NAME)
    }

    pub(super) fn mk_index_path(&self, slot_height: u64) -> PathBuf {
        self.mk_slot_path(slot_height).join(INDEX_FILE_NAME)
    }

    pub(super) fn mk_meta_path(&self, slot_height: u64) -> PathBuf {
        self.mk_slot_path(slot_height).join(META_FILE_NAME)
    }

    pub(super) fn open_data_at(&self, slot_height: u64, blob_index: u64) -> Result<File> {
        let slot_path = self.mk_slot_path(slot_height);
        let (data_path, index_path) = (
            slot_path.join(DATA_FILE_NAME),
            slot_path.join(INDEX_FILE_NAME),
        );

        // locate
        let (data_path, blob_loc) = self
            .index_data(slot_height, blob_index)?
            .ok_or(BlobStoreError::NoSuchBlob)?;
        let mut data_file = File::open(data_path)?;

        data_file.seek(SeekFrom::Start(blob_loc.offset))?;

        Ok(data_file)
    }

    /// returns a tuple of `(data_path, blob_loc )` by scanning index file
    // TODO: possibly optimize by checking metadata and immediately quiting based on too big indices
    pub(super) fn index_data(
        &self,
        slot_height: u64,
        blob_index: u64,
    ) -> Result<Option<(PathBuf, BlobIndex)>> {
        let slot_path = self.mk_slot_path(slot_height);
        let (data_path, index_path) = (
            slot_path.join(DATA_FILE_NAME),
            slot_path.join(INDEX_FILE_NAME),
        );

        let mut index_file = BufReader::new(File::open(&index_path)?);

        let mut buf = [0u8; INDEX_RECORD_SIZE];
        while let Ok(_) = index_file.read_exact(&mut buf) {
            let index = BigEndian::read_u64(&buf[0..8]);
            if index == blob_index {
                let offset = BigEndian::read_u64(&buf[8..16]);
                let size = BigEndian::read_u64(&buf[16..24]);
                return Ok(Some((
                    data_path,
                    BlobIndex {
                        index,
                        offset,
                        size,
                    },
                )));
            }
        }

        Ok(None)
    }

    pub(super) fn ordered_indices(&self, slot_height: u64) -> Result<Vec<BlobIndex>> {
        let slot_path = self.mk_slot_path(slot_height);
        let index_path = slot_path.join(INDEX_FILE_NAME);

        let mut index_file = File::open(&index_path)?;
        let file_size = index_file.metadata()?.len();

        let mut buf = [0u8; INDEX_RECORD_SIZE];
        let mut indices = Vec::new();

        while let Ok(_) = index_file.read_exact(&mut buf) {
            let index = BigEndian::read_u64(&buf[0..8]);
            let offset = BigEndian::read_u64(&buf[8..16]);

            let size = BigEndian::read_u64(&buf[16..24]);
            indices.push(BlobIndex {
                index,
                offset,
                size,
            });
        }

        assert_eq!(
            (indices.len() * INDEX_RECORD_SIZE) as u64,
            file_size,
            "failed to read all index records"
        );

        indices.sort_unstable();

        Ok(indices)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BlobIndex {
    pub index: u64,
    pub offset: u64,
    pub size: u64,
}

impl PartialOrd for BlobIndex {
    fn partial_cmp(&self, other: &BlobIndex) -> Option<std::cmp::Ordering> {
        self.index.partial_cmp(&other.index)
    }
}

impl Ord for BlobIndex {
    fn cmp(&self, other: &BlobIndex) -> std::cmp::Ordering {
        self.index.cmp(&other.index)
    }
}

struct BlobData<'a> {
    indices: Vec<BlobIndex>,
    position: u64,
    data_file: BufReader<File>,
    _fake: PhantomData<&'a BlobStore>,
}

impl<'a> Iterator for BlobData<'a> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.indices.len() as u64 {
            None
        } else {
            let blob_index = &self.indices[self.position as usize];

            match self.data_file.seek(SeekFrom::Start(blob_index.offset)) {
                Ok(_offset) => {}
                Err(e) => {
                    return Some(Err(BlobStoreError::from(e)));
                }
            }
            let mut buf = vec![0; blob_index.size as usize];
            self.position += 1;

            match self.data_file.read_exact(&mut buf[..]) {
                Ok(_) => Some(Ok(buf)),
                Err(e) => Some(Err(e.into())),
            }
        }
    }
}

pub(super) fn bad_blob(e: crate::result::Error) -> BlobStoreError {
    BlobStoreError::BadBlob
}

pub(super) fn meta_key(slot_height: u64) -> Vec<u8> {
    let mut key = vec![0u8; 8];
    BigEndian::write_u64(&mut key[0..8], slot_height);
    key
}

pub(super) fn data_key(slot_height: u64, index: u64) -> Vec<u8> {
    let mut key = vec![0u8; 16];
    BigEndian::write_u64(&mut key[0..8], slot_height);
    BigEndian::write_u64(&mut key[8..16], index);
    key
}

pub(super) fn data_index_from_key(key: &[u8]) -> Result<u64> {
    let mut rdr = io::Cursor::new(&key[8..16]);
    let index = rdr.read_u64::<BigEndian>()?;
    Ok(index)
}

pub(super) fn erasure_key(slot_height: u64, index: u64) -> Vec<u8> {
    data_key(slot_height, index)
}
