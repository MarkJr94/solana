use crate::contact_info::ContactInfo;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signable, Signature};
use solana_sdk::transaction::Transaction;
use std::fmt;

/// CrdsValue that is replicated across the cluster
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum CrdsValue {
    /// * Merge Strategy - Latest wallclock is picked
    ContactInfo(ContactInfo),
    /// * Merge Strategy - Latest wallclock is picked
    Vote(Vote),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Vote {
    pub transaction: Transaction,
    pub wallclock: u64,
}

impl Signable for Vote {
    fn sign(&mut self, _keypair: &Keypair) {}

    fn verify(&self) -> bool {
        self.transaction.verify_signature()
    }

    fn pubkey(&self) -> Pubkey {
        self.transaction.account_keys[0]
    }

    fn signable_data(&self) -> Vec<u8> {
        vec![]
    }

    fn get_signature(&self) -> Signature {
        Signature::default()
    }

    fn set_signature(&mut self, _signature: Signature) {}
}

/// Type of the replicated value
/// These are labels for values in a record that is associated with `Pubkey`
#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub enum CrdsValueLabel {
    ContactInfo(Pubkey),
    Vote(Pubkey),
}

impl fmt::Display for CrdsValueLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrdsValueLabel::ContactInfo(_) => write!(f, "ContactInfo({})", self.pubkey()),
            CrdsValueLabel::Vote(_) => write!(f, "Vote({})", self.pubkey()),
        }
    }
}

impl CrdsValueLabel {
    pub fn pubkey(&self) -> Pubkey {
        match self {
            CrdsValueLabel::ContactInfo(p) => *p,
            CrdsValueLabel::Vote(p) => *p,
        }
    }
}

impl Vote {
    // TODO: it might make sense for the transaction to encode the wallclock in the data
    pub fn new(transaction: Transaction, wallclock: u64) -> Self {
        Vote {
            transaction,
            wallclock,
        }
    }
}

impl CrdsValue {
    /// Totally unsecure unverfiable wallclock of the node that generated this message
    /// Latest wallclock is always picked.
    /// This is used to time out push messages.
    pub fn wallclock(&self) -> u64 {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.wallclock,
            CrdsValue::Vote(vote) => vote.wallclock,
        }
    }
    pub fn label(&self) -> CrdsValueLabel {
        match self {
            CrdsValue::ContactInfo(contact_info) => {
                CrdsValueLabel::ContactInfo(contact_info.pubkey())
            }
            CrdsValue::Vote(vote) => CrdsValueLabel::Vote(vote.pubkey()),
        }
    }
    pub fn contact_info(&self) -> Option<&ContactInfo> {
        match self {
            CrdsValue::ContactInfo(contact_info) => Some(contact_info),
            _ => None,
        }
    }
    pub fn vote(&self) -> Option<&Vote> {
        match self {
            CrdsValue::Vote(vote) => Some(vote),
            _ => None,
        }
    }
    /// Return all the possible labels for a record identified by Pubkey.
    pub fn record_labels(key: &Pubkey) -> [CrdsValueLabel; 2] {
        [
            CrdsValueLabel::ContactInfo(*key),
            CrdsValueLabel::Vote(*key),
        ]
    }
}

impl Signable for CrdsValue {
    fn sign(&mut self, keypair: &Keypair) {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.sign(keypair),
            CrdsValue::Vote(vote) => vote.sign(keypair),
        };
    }
    fn verify(&self) -> bool {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.verify(),
            CrdsValue::Vote(vote) => vote.verify(),
        }
    }

    fn pubkey(&self) -> Pubkey {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.pubkey(),
            CrdsValue::Vote(vote) => vote.pubkey(),
        }
    }

    fn signable_data(&self) -> Vec<u8> {
        unimplemented!()
    }

    fn get_signature(&self) -> Signature {
        unimplemented!()
    }

    fn set_signature(&mut self, _: Signature) {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::contact_info::ContactInfo;
    use crate::test_tx::test_tx;
    use solana_sdk::signature::{Keypair, KeypairUtil};
    use solana_sdk::timing::timestamp;

    #[test]
    fn test_labels() {
        let mut hits = [false; 2];
        // this method should cover all the possible labels
        for v in &CrdsValue::record_labels(&Pubkey::default()) {
            match v {
                CrdsValueLabel::ContactInfo(_) => hits[0] = true,
                CrdsValueLabel::Vote(_) => hits[1] = true,
            }
        }
        assert!(hits.iter().all(|x| *x));
    }
    #[test]
    fn test_keys_and_values() {
        let v = CrdsValue::ContactInfo(ContactInfo::default());
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().contact_info().unwrap().id;
        assert_eq!(v.label(), CrdsValueLabel::ContactInfo(key));

        let v = CrdsValue::Vote(Vote::new(test_tx(), 0));
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().vote().unwrap().transaction.account_keys[0];
        assert_eq!(v.label(), CrdsValueLabel::Vote(key));
    }
    #[test]
    fn test_signature() {
        let keypair = Keypair::new();
        let fake_keypair = Keypair::new();
        let mut v =
            CrdsValue::ContactInfo(ContactInfo::new_localhost(&keypair.pubkey(), timestamp()));
        v.sign(&keypair);
        assert!(v.verify());
        v.sign(&fake_keypair);
        assert!(!v.verify());
    }

}
