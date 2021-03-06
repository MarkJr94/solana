use crate::native_loader;
use solana_sdk::account::{create_keyed_accounts, Account, KeyedAccount};
use solana_sdk::native_program::ProgramError;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_program;
use solana_sdk::transaction::{InstructionError, Transaction, TransactionError};

/// Process an instruction
/// This method calls the instruction's program entrypoint method
fn process_instruction(
    tx: &Transaction,
    instruction_index: usize,
    executable_accounts: &mut [(Pubkey, Account)],
    program_accounts: &mut [&mut Account],
    tick_height: u64,
) -> Result<(), ProgramError> {
    let program_id = tx.program_id(instruction_index);

    let mut keyed_accounts = create_keyed_accounts(executable_accounts);
    let mut keyed_accounts2: Vec<_> = tx.instructions[instruction_index]
        .accounts
        .iter()
        .map(|&index| {
            let index = index as usize;
            let key = &tx.account_keys[index];
            (key, index < tx.signatures.len())
        })
        .zip(program_accounts.iter_mut())
        .map(|((key, is_signer), account)| KeyedAccount::new(key, is_signer, account))
        .collect();
    keyed_accounts.append(&mut keyed_accounts2);

    if system_program::check_id(&program_id) {
        crate::system_program::entrypoint(
            &program_id,
            &mut keyed_accounts[1..],
            &tx.instructions[instruction_index].data,
            tick_height,
        )
    } else {
        native_loader::entrypoint(
            &program_id,
            &mut keyed_accounts,
            &tx.instructions[instruction_index].data,
            tick_height,
        )
    }
}

fn verify_instruction(
    program_id: &Pubkey,
    pre_program_id: &Pubkey,
    pre_lamports: u64,
    pre_data: &[u8],
    account: &Account,
) -> Result<(), InstructionError> {
    // Verify the transaction

    // Make sure that program_id is still the same or this was just assigned by the system program
    if *pre_program_id != account.owner && !system_program::check_id(&program_id) {
        return Err(InstructionError::ModifiedProgramId);
    }
    // For accounts unassigned to the program, the individual balance of each accounts cannot decrease.
    if *program_id != account.owner && pre_lamports > account.lamports {
        return Err(InstructionError::ExternalAccountLamportSpend);
    }
    // For accounts unassigned to the program, the data may not change.
    if *program_id != account.owner
        && !system_program::check_id(&program_id)
        && pre_data != &account.data[..]
    {
        return Err(InstructionError::ExternalAccountDataModified);
    }
    Ok(())
}

/// Execute an instruction
/// This method calls the instruction's program entrypoint method and verifies that the result of
/// the call does not violate the bank's accounting rules.
/// The accounts are committed back to the bank only if this function returns Ok(_).
fn execute_instruction(
    tx: &Transaction,
    instruction_index: usize,
    executable_accounts: &mut [(Pubkey, Account)],
    program_accounts: &mut [&mut Account],
    tick_height: u64,
) -> Result<(), InstructionError> {
    let program_id = tx.program_id(instruction_index);
    // TODO: the runtime should be checking read/write access to memory
    // we are trusting the hard-coded programs not to clobber or allocate
    let pre_total: u64 = program_accounts.iter().map(|a| a.lamports).sum();
    let pre_data: Vec<_> = program_accounts
        .iter_mut()
        .map(|a| (a.owner, a.lamports, a.data.clone()))
        .collect();

    process_instruction(
        tx,
        instruction_index,
        executable_accounts,
        program_accounts,
        tick_height,
    )
    .map_err(verify_error)
    .map_err(InstructionError::ProgramError)?;

    // Verify the instruction
    for ((pre_program_id, pre_lamports, pre_data), post_account) in
        pre_data.iter().zip(program_accounts.iter())
    {
        verify_instruction(
            &program_id,
            pre_program_id,
            *pre_lamports,
            pre_data,
            post_account,
        )?;
    }
    // The total sum of all the lamports in all the accounts cannot change.
    let post_total: u64 = program_accounts.iter().map(|a| a.lamports).sum();
    if pre_total != post_total {
        return Err(InstructionError::UnbalancedInstruction);
    }
    Ok(())
}

/// Return true if the slice has any duplicate elements
pub fn has_duplicates<T: PartialEq>(xs: &[T]) -> bool {
    // Note: This is an O(n^2) algorithm, but requires no heap allocations. The benchmark
    // `bench_has_duplicates` in benches/runtime.rs shows that this implementation is
    // ~50 times faster than using HashSet for very short slices.
    for i in 1..xs.len() {
        if xs[i..].contains(&xs[i - 1]) {
            return true;
        }
    }
    false
}

/// Get mut references to a subset of elements.
fn get_subset_unchecked_mut<'a, T>(
    xs: &'a mut [T],
    indexes: &[u8],
) -> Result<Vec<&'a mut T>, InstructionError> {
    // Since the compiler doesn't know the indexes are unique, dereferencing
    // multiple mut elements is assumed to be unsafe. If, however, all
    // indexes are unique, it's perfectly safe. The returned elements will share
    // the liftime of the input slice.

    // Make certain there are no duplicate indexes. If there are, return an error
    // because we can't return multiple mut references to the same element.
    if has_duplicates(indexes) {
        return Err(InstructionError::DuplicateAccountIndex);
    }

    Ok(indexes
        .iter()
        .map(|i| {
            let ptr = &mut xs[*i as usize] as *mut T;
            unsafe { &mut *ptr }
        })
        .collect())
}

/// Execute a transaction.
/// This method calls each instruction in the transaction over the set of loaded Accounts
/// The accounts are committed back to the bank only if every instruction succeeds
pub fn execute_transaction(
    tx: &Transaction,
    loaders: &mut [Vec<(Pubkey, Account)>],
    tx_accounts: &mut [Account],
    tick_height: u64,
) -> Result<(), TransactionError> {
    for (instruction_index, instruction) in tx.instructions.iter().enumerate() {
        let executable_accounts = &mut (&mut loaders[instruction.program_ids_index as usize]);
        let mut program_accounts = get_subset_unchecked_mut(tx_accounts, &instruction.accounts)
            .map_err(|err| TransactionError::InstructionError(instruction_index as u8, err))?;
        execute_instruction(
            tx,
            instruction_index,
            executable_accounts,
            &mut program_accounts,
            tick_height,
        )
        .map_err(|err| TransactionError::InstructionError(instruction_index as u8, err))?;
    }
    Ok(())
}

/// A utility function for unit-tests. Same as execute_transaction(), but bypasses the loaders
/// for easier usage and better stack traces.
pub fn process_transaction<F, E>(
    tx: &Transaction,
    tx_accounts: &mut Vec<Account>,
    process_instruction: F,
) -> Result<(), E>
where
    F: Fn(&Pubkey, &mut [KeyedAccount], &[u8]) -> Result<(), E>,
{
    for _ in tx_accounts.len()..tx.account_keys.len() {
        tx_accounts.push(Account::new(0, 0, &system_program::id()));
    }
    for (i, ix) in tx.instructions.iter().enumerate() {
        let mut ix_accounts = get_subset_unchecked_mut(tx_accounts, &ix.accounts).unwrap();
        let mut keyed_accounts: Vec<_> = ix
            .accounts
            .iter()
            .map(|&index| {
                let index = index as usize;
                let key = &tx.account_keys[index];
                (key, index < tx.signatures.len())
            })
            .zip(ix_accounts.iter_mut())
            .map(|((key, is_signer), account)| KeyedAccount::new(key, is_signer, account))
            .collect();

        let program_id = tx.program_id(i);
        if system_program::check_id(&program_id) {
            crate::system_program::entrypoint(&program_id, &mut keyed_accounts, &ix.data, 0)
                .unwrap();
        } else {
            process_instruction(&program_id, &mut keyed_accounts, &ix.data)?;
        }
    }
    Ok(())
}

fn verify_error(err: ProgramError) -> ProgramError {
    match err {
        ProgramError::CustomError(mut error) => {
            error.truncate(32);
            ProgramError::CustomError(error)
        }
        e => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::{Keypair, KeypairUtil};

    #[test]
    fn test_has_duplicates() {
        assert!(!has_duplicates(&[1, 2]));
        assert!(has_duplicates(&[1, 2, 1]));
    }

    #[test]
    fn test_get_subset_unchecked_mut() {
        assert_eq!(
            get_subset_unchecked_mut(&mut [7, 8], &[0]).unwrap(),
            vec![&mut 7]
        );
        assert_eq!(
            get_subset_unchecked_mut(&mut [7, 8], &[0, 1]).unwrap(),
            vec![&mut 7, &mut 8]
        );
    }

    #[test]
    fn test_get_subset_unchecked_mut_duplicate_index() {
        // This panics, because it assumes duplicate detection is done elsewhere.
        assert_eq!(
            get_subset_unchecked_mut(&mut [7, 8], &[0, 0]).unwrap_err(),
            InstructionError::DuplicateAccountIndex
        );
    }

    #[test]
    #[should_panic]
    fn test_get_subset_unchecked_mut_out_of_bounds() {
        // This panics, because it assumes bounds validation is done elsewhere.
        get_subset_unchecked_mut(&mut [7, 8], &[2]).unwrap();
    }

    #[test]
    fn test_verify_instruction_change_program_id() {
        fn change_program_id(
            ix: &Pubkey,
            pre: &Pubkey,
            post: &Pubkey,
        ) -> Result<(), InstructionError> {
            verify_instruction(&ix, &pre, 0, &[], &Account::new(0, 0, post))
        }

        let system_program_id = system_program::id();
        let alice_program_id = Keypair::new().pubkey();
        let mallory_program_id = Keypair::new().pubkey();

        assert_eq!(
            change_program_id(&system_program_id, &system_program_id, &alice_program_id),
            Ok(()),
            "system program should be able to change the account owner"
        );
        assert_eq!(
            change_program_id(&mallory_program_id, &system_program_id, &alice_program_id),
            Err(InstructionError::ModifiedProgramId),
            "malicious Mallory should not be able to change the account owner"
        );
    }

    #[test]
    fn test_verify_instruction_change_data() {
        fn change_data(program_id: &Pubkey) -> Result<(), InstructionError> {
            let alice_program_id = Keypair::new().pubkey();
            let account = Account::new(0, 0, &alice_program_id);
            verify_instruction(&program_id, &alice_program_id, 0, &[42], &account)
        }

        let system_program_id = system_program::id();
        let mallory_program_id = Keypair::new().pubkey();

        assert_eq!(
            change_data(&system_program_id),
            Ok(()),
            "system program should be able to change the data"
        );
        assert_eq!(
            change_data(&mallory_program_id),
            Err(InstructionError::ExternalAccountDataModified),
            "malicious Mallory should not be able to change the account data"
        );
    }

    #[test]
    fn test_verify_error() {
        let short_error = ProgramError::CustomError(vec![1, 2, 3]);
        let expected_short_error = short_error.clone(); // short CustomError errors should be untouched
        assert_eq!(verify_error(short_error), expected_short_error);

        let long_error = ProgramError::CustomError(vec![8; 40]);
        let expected_long_error = ProgramError::CustomError(vec![8; 32]); // long CustomError errors should be truncated
        assert_eq!(verify_error(long_error), expected_long_error);

        let other_error = ProgramError::GenericError;
        let expected_other_error = other_error.clone(); // non-CustomError errors should be untouched
        assert_eq!(verify_error(other_error), expected_other_error);
    }
}
