use std::sync::Arc;

use indicatif::ProgressBar;

use common_config_parser::types::Config;
use core_consensus::engine::generate_receipts_and_logs;
use core_executor::{MPTTrie, RocksTrieDB};
use core_storage::{adapter::rocks::RocksAdapter, ImplStorage};
use protocol::{
    async_trait,
    traits::{CommonStorage as _, Context, Storage},
    types::{ExecResp, RichBlock},
    ProtocolError, ProtocolResult,
};

use super::Migration;
use crate::{execute_transactions, insert_accounts, save_block, MainError};

const VERSION: &str = "20230803150000";

/// Before this version, the genesis receipts are not saved into blocks.
pub struct SaveGenesisReceipts;

async fn execute_genesis(config: &Config, genesis: &RichBlock) -> ProtocolResult<ExecResp> {
    let tmp_dir = tempfile::tempdir().map_err(|err| {
        let errmsg = format!("failed to create temporary directory since {err:?}");
        MainError::Other(errmsg)
    })?;

    let storage = {
        let path_block = tmp_dir.path().join("block");
        let rocks_adapter = Arc::new(RocksAdapter::new(path_block, config.rocksdb.clone())?);
        let impl_storage = ImplStorage::new(rocks_adapter, config.rocksdb.cache_size);
        Arc::new(impl_storage)
    };

    let trie_db = {
        let path_state = tmp_dir.path().join("state");
        let trie_db = RocksTrieDB::new(
            path_state,
            config.rocksdb.clone(),
            config.executor.triedb_cache_size,
        )?;
        Arc::new(trie_db)
    };
    let state_root = {
        let mut mpt = MPTTrie::new(Arc::clone(&trie_db));
        insert_accounts(&mut mpt, &config.accounts)?;
        mpt.commit()?
    };

    let path_metadata = tmp_dir.path().join("metadata");
    let resp = execute_transactions(
        genesis,
        state_root,
        &trie_db,
        &storage,
        path_metadata,
        &config.rocksdb.clone(),
    )
    .await?;

    Ok(resp)
}

impl SaveGenesisReceipts {
    async fn migrate_internal(
        &self,
        storage: &Arc<ImplStorage<RocksAdapter>>,
        config: &Config,
        genesis: &RichBlock,
    ) -> Result<(), ProtocolError> {
        let (rich, has_genesis) = {
            if let Some(block) = storage.get_block(Context::new(), 0).await? {
                let txs = storage
                    .get_transactions(Context::new(), 0, &block.tx_hashes)
                    .await?
                    .into_iter()
                    .enumerate()
                    .try_fold(Vec::new(), |mut txs, (index, tx_opt)| {
                        if let Some(tx) = tx_opt {
                            txs.push(tx);
                            Ok(txs)
                        } else {
                            let errmsg = format!(
                                "the data of {index}-th transaction in genesis block is not found"
                            );
                            Err(MainError::Other(errmsg))
                        }
                    })?;
                (RichBlock { block, txs }, true)
            } else {
                (genesis.clone(), false)
            }
        };
        let resp = execute_genesis(&config, &rich).await?;

        let (receipts, _logs) = generate_receipts_and_logs(
            rich.block.header.number,
            rich.block.header.hash(),
            rich.block.header.state_root,
            &rich.txs,
            &resp,
        );

        if has_genesis {
            storage
                .insert_receipts(Context::new(), rich.block.header.number, receipts)
                .await?;
        } else {
            save_block(storage, &rich, &resp).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Migration for SaveGenesisReceipts {
    async fn migrate(
        &self,
        storage: &Arc<ImplStorage<RocksAdapter>>,
        config: &Config,
        genesis: &RichBlock,
        _pb: Arc<dyn Fn(u64) -> ProgressBar + Send + Sync>,
    ) -> Result<(), ProtocolError> {
        self.migrate_internal(storage, config, genesis).await
    }

    fn version(&self) -> &str {
        VERSION
    }

    fn is_expensive(&self) -> bool {
        false
    }

    fn note(&self) -> Option<&str> {
        None
    }
}
