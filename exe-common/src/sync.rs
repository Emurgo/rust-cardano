use cardano::block::{
    Block, BlockDate, BlockHeader, ChainState, EpochFlags, EpochId, Error as BlockError,
    HeaderHash, RawBlock,
};
use cardano::config::GenesisData;
use cardano::util::hex;
use cardano_storage::{
    blob, chain_state,
    epoch::{self, epoch_exists},
    pack, tag, types, Error, Storage, StorageConfig,
};
use config::net;
use network::{
    api::{Api, BlockReceivingFlag, BlockRef},
    Error::ProtocolError,
    Peer, Result,
};
use protocol::Error::ServerError;
use std::cmp::min;
use std::mem;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use storage_units::packfile;

fn duration_print(d: Duration) -> String {
    format!("{}.{:03} seconds", d.as_secs(), d.subsec_millis())
}

struct EpochWriterState {
    epoch_id: EpochId,
    writer: packfile::Writer,
    write_start_time: SystemTime,
    blobs_to_delete: Vec<HeaderHash>,
}

fn net_sync_to<A: Api>(
    net: &mut A,
    net_cfg: &net::Config,
    genesis_data: &GenesisData,
    storage: Arc<RwLock<Storage>>,
    tip_header: &BlockHeader,
) -> Result<()> {
    let tip = BlockRef {
        hash: tip_header.compute_hash(),
        parent: tip_header.get_previous_header(),
        date: tip_header.get_blockdate(),
    };
    let storage_config = storage.read().unwrap().config.clone();

    debug!("Configured genesis   : {}", net_cfg.genesis);
    debug!("Configured genesis-1 : {}", net_cfg.genesis_prev);
    info!(
        "Network TIP is       : {} ({}) <- {}",
        tip.hash, tip.date, tip.parent
    );

    // Start fetching at the current HEAD tag, or the genesis block if
    // it doesn't exist.
    let (our_tip, our_tip_is_genesis) = match storage.read().unwrap().get_block_from_tag(&tag::HEAD)
    {
        Err(Error::NoSuchTag) => (
            BlockRef {
                hash: net_cfg.genesis.clone(),
                parent: net_cfg.genesis_prev.clone(),
                date: BlockDate::Boundary(net_cfg.epoch_start),
            },
            true,
        ),
        Err(err) => panic!(err),
        Ok(block) => {
            let header = block.header();
            (
                BlockRef {
                    hash: header.compute_hash(),
                    parent: header.previous_header(),
                    date: header.blockdate(),
                },
                false,
            )
        }
    };

    let mut is_epoch_with_ebb = false;

    // TODO: we need to handle the case where our_tip is not an
    // ancestor of tip. In that case we should start from the last
    // stable epoch before our_tip.

    info!("Fetching from        : {} ({})", our_tip.hash, our_tip.date);

    // Determine whether the previous epoch is stable yet. Note: This
    // assumes that k is smaller than the number of blocks in an
    // epoch.
    let first_unstable_epoch = tip.date.get_epochid()
        - match tip.date {
            BlockDate::Boundary(_) => 1,
            BlockDate::Normal(d) => {
                if d.slotid as usize <= net_cfg.epoch_stability_depth {
                    1
                } else {
                    0
                }
            }
        };
    info!("First unstable epoch : {}", first_unstable_epoch);

    let mut epoch_writer_state: Option<EpochWriterState> = None;

    // If our tip is in an epoch that has become stable, we now need
    // to pack it. So read the previously fetched blocks in this epoch
    // and prepend them to the incoming blocks.
    if our_tip.date.get_epochid() < first_unstable_epoch
        && !our_tip_is_genesis
        && !epoch_exists(&storage_config, our_tip.date.get_epochid()).unwrap()
    {
        let epoch_id = our_tip.date.get_epochid();

        // Read the blocks in the current epoch.
        let mut blobs_to_delete = vec![];
        let (last_block_in_prev_epoch, blocks, is_ebb_present) = get_unpacked_blocks_in_epoch(
            &storage.read().unwrap(),
            &our_tip.hash,
            epoch_id,
            &mut blobs_to_delete,
        );
        is_epoch_with_ebb = is_ebb_present;

        // If tip.slotid < w, the previous epoch won't have been
        // created yet either, so do that now.
        if epoch_id > net_cfg.epoch_start {
            maybe_create_epoch(
                &mut storage.write().unwrap(),
                genesis_data,
                epoch_id - 1,
                &last_block_in_prev_epoch,
            )?;
        }

        info!("Finalising open epoch that became stable");
        // Initialize the epoch writer and add the blocks in the current epoch.
        epoch_writer_state = Some(EpochWriterState {
            epoch_id,
            writer: pack::packwriter_init(&storage_config).unwrap(),
            write_start_time: SystemTime::now(),
            blobs_to_delete,
        });

        let mut chain_state = chain_state::restore_chain_state(
            &storage.read().unwrap(),
            genesis_data,
            &last_block_in_prev_epoch,
        )?;

        append_blocks_to_epoch_reverse(
            epoch_writer_state.as_mut().unwrap(),
            &mut chain_state,
            blocks,
        )?;
    }
    // If the previous epoch has become stable, then we may need to
    // pack it.
    else if our_tip.date.get_epochid() == first_unstable_epoch
        && first_unstable_epoch > net_cfg.epoch_start
        && !epoch_exists(&storage_config, first_unstable_epoch - 1).unwrap()
    {
        // Iterate to the last block in the previous epoch.
        let mut cur_hash = our_tip.hash.clone();
        loop {
            let block_raw = storage
                .read()
                .unwrap()
                .read_block(&cur_hash.clone().into())
                .unwrap();
            let block = block_raw.decode().unwrap();
            let hdr = block.header();
            let blockdate = hdr.blockdate();
            let from_this_epoch = blockdate.get_epochid() == first_unstable_epoch;
            // switch to previous block if still in this epoch
            if from_this_epoch {
                cur_hash = hdr.previous_header();
            }
            // terminate if EBB or there isn't one and already another epoch
            if blockdate.is_boundary() || !from_this_epoch {
                break;
            }
        }

        maybe_create_epoch(
            &mut storage.write().unwrap(),
            genesis_data,
            first_unstable_epoch - 1,
            &cur_hash,
        )?;
    }

    let mut chain_state = chain_state::restore_chain_state(
        &storage.read().unwrap(),
        genesis_data,
        if our_tip_is_genesis {
            &our_tip.parent
        } else {
            &our_tip.hash
        },
    )?;
    let mut is_rollback = false;
    if let Some(last_ebb_epoch) = chain_state.last_boundary_block_epoch {
        is_epoch_with_ebb = last_ebb_epoch == our_tip.date.get_epochid();
    }

    let blocks_response = net.get_blocks(
        &our_tip,
        our_tip_is_genesis,
        &tip,
        &mut |block_hash, block, block_raw| {
            if is_rollback {
                return BlockReceivingFlag::Stop;
            }

            // Clone current chain state before validation
            let chain_state_before = chain_state.clone();

            let date = block.header().blockdate();

            // Validate block and update current chain state
            // FIXME: propagate errors
            match chain_state.verify_block(block_hash, block) {
                Ok(_) => {}
                Err(BlockError::WrongPreviousBlock(actual, expected)) => {
                    debug!(
                        "Detected fork after request with valid local tip: expected parent is {} but actual parent is {} at date {:?}",
                        expected.as_hex(),
                        actual.as_hex(),
                        date
                    );
                    chain_state = chain_state_before;
                    is_rollback = true;
                    return BlockReceivingFlag::Stop;
                }
                Err(err) => panic!(
                    "Block {} ({}) failed to verify: {:?}",
                    hex::encode(block_hash.as_hash_bytes()),
                    date,
                    err
                ),
            }

            // Flush the previous epoch (if any)

            // Calculate if this is a start of a new epoch (including the very first one)
            // And if there's any previous date available (previous epochs)
            let (is_new_epoch_start, is_prev_date_exists) = match chain_state_before.last_date {
                None => (true, false),
                Some(last_date) => (date.get_epochid() > last_date.get_epochid(), true),
            };

            if is_new_epoch_start {
                if is_prev_date_exists {
                    let mut writer_state = None;
                    mem::swap(&mut writer_state, &mut epoch_writer_state);

                    if let Some(epoch_writer_state) = writer_state {
                        finish_epoch(
                            &mut storage.write().unwrap(),
                            genesis_data,
                            epoch_writer_state,
                            &chain_state_before,
                            is_epoch_with_ebb,
                        )
                            .unwrap();

                        // Checkpoint the tip so we don't have to refetch
                        // everything if we get interrupted.
                        tag::write(
                            &storage.read().unwrap(),
                            &tag::HEAD,
                            &chain_state_before.last_block.as_ref(),
                        );
                    }
                }
                is_epoch_with_ebb = date.is_boundary()
            }

            if date.get_epochid() >= first_unstable_epoch {
                // This block is not part of a stable epoch yet and could
                // be rolled back. Therefore we can't pack this epoch
                // yet. Instead we write this block to disk separately.
                let block_hash = types::header_to_blockhash(&block_hash);
                match storage.write() {
                    Ok(mut storage) => {
                        blob::write(&storage, &block_hash, block_raw.as_ref()).unwrap();
                        // Add loose block to index
                        storage.add_loose_to_index(&block.header());
                    }
                    Err(err) => panic!("Failed to acquire storage writer lock! {:?}", err),
                }
            } else {
                // If this is the epoch genesis block, start writing a new epoch pack.
                if is_new_epoch_start {
                    epoch_writer_state = Some(EpochWriterState {
                        epoch_id: date.get_epochid(),
                        writer: pack::packwriter_init(&storage_config).unwrap(),
                        write_start_time: SystemTime::now(),
                        blobs_to_delete: vec![],
                    });
                }

                // And append the block to the epoch pack.
                if let Some(epoch_writer_state) = epoch_writer_state.as_mut() {
                    epoch_writer_state
                        .writer
                        .append(&types::header_to_blockhash(&block_hash), block_raw.as_ref())
                        .unwrap();
                } else {
                    unreachable!();
                }
            }
            return BlockReceivingFlag::Continue;
        },
    );

    if is_rollback || blocks_response_is_rollback(blocks_response, &our_tip) {
        if let Some(last_date) = chain_state.last_date {
            // Perform rollback
            let mut new_tip_hash = perform_rollback(
                &storage_config,
                storage.clone(),
                &net_cfg,
                &last_date,
                first_unstable_epoch,
            )?;
            // Restore new chain state after the rollback
            chain_state = chain_state::restore_chain_state(
                &storage.read().unwrap(),
                genesis_data,
                &new_tip_hash,
            )?;
        } else {
            panic!("Rollback at the very chain start");
        }
    }

    // Update the tip tag to point to the most recent block.
    tag::write(
        &storage.read().unwrap(),
        &tag::HEAD,
        chain_state.last_block.as_ref(),
    );

    Ok(())
}

fn blocks_response_is_rollback(resp: Result<()>, our_tip: &BlockRef) -> bool {
    match resp {
        Ok(()) => false,
        Err(e) => {
            if let ProtocolError(ServerError(s)) = &e {
                if s.contains("Failed to find lca") {
                    warn!(
                        "Detected fork: local tip is no longer part of the chain: {:?}",
                        our_tip
                    );
                    return true;
                }
            }
            panic!("`net.get_blocks` error: {:?}", e);
        }
    }
}

fn perform_rollback(
    storage_config: &StorageConfig,
    storage: Arc<RwLock<Storage>>,
    net_cfg: &net::Config,
    last_date: &BlockDate,
    first_unstable_epoch: EpochId,
) -> Result<HeaderHash> {
    if last_date.get_epochid() >= first_unstable_epoch {
        // We are syncing along with the network and rollback is in loose blocks
        // drop `min(K, len(loose))` loose blocks and retry
        match storage.write() {
            Ok(mut storage) => {
                let len = storage.loose_index_len();
                if len == 0 {
                    panic!("Last block is from unstable epoch, but loose index len is 0!")
                }
                // We drop either whole index, or just stability tail
                let drop = min(len, net_cfg.epoch_stability_depth);
                storage
                    .loose_index_drop_from_head(drop, true)
                    .expect("Failed to drop loose index head!");
                // Find new tip after rollback
                let tip = if len > drop {
                    // New tip is last loose block
                    storage.loose_index_tip().unwrap().header_hash()
                } else {
                    let epochs = storage.packed_epochs_len() as u64;
                    restore_previous_tip_for_epoch(epochs, net_cfg, storage_config)
                };
                return Ok(tip);
            }
            Err(e) => panic!("Can't lock storage! {:?}", e),
        }
    } else {
        // Either we are syncing historical data and rollback is in old epoch
        // Or we dropped all loose blocks and now latest tip is in packed epoch
        let current_epoch = last_date.get_epochid();
        match storage.write() {
            Ok(mut storage) => {
                let last_packed_epoch = (storage.packed_epochs_len() - 1) as u64;
                if current_epoch == last_packed_epoch {
                    // We are inside last packed epoch
                    // Drop current epoch and roll back to previous one
                    storage.drop_packed_epoch(current_epoch)?;
                } else {
                    panic!(
                        "Rollback while in a stable epoch, but current epoch does not match last packed epoch! (current_epoch={}, last_packed_epoch={})",
                        current_epoch,
                        last_packed_epoch,
                    );
                }
            }
            Err(e) => panic!("Can't lock storage! {:?}", e),
        }
        return Ok(restore_previous_tip_for_epoch(
            current_epoch,
            net_cfg,
            storage_config,
        ));
    }
}

fn restore_previous_tip_for_epoch(
    epoch: u64,
    net_cfg: &net::Config,
    storage_config: &StorageConfig,
) -> HeaderHash {
    if epoch == 0 {
        // If there are no packed epochs - new tip is genesis
        net_cfg.genesis.clone()
    } else {
        // New tip is last block of last packed epoch
        let prev_epoch = (epoch - 1) as u64;
        epoch::epoch_read_chainstate_ref(&storage_config, prev_epoch).expect(&format!(
            "Failed to read chainstate ref from epoch {}",
            prev_epoch
        ))
    }
}

/// Synchronize the local blockchain stored in `storage` with the
/// network `net`. That is, fetch all blocks between the most recent
/// block we received (as denoted by the `HEAD` tag) and the network's
/// current tip. Blocks will be packed into epochs on disk as soon
/// they're stable.
///
/// If `sync_once` is set to `true`, then this function will
/// synchronize once and then return. If it's set to `false`, then
/// this function will run forever, continuously synchronizing to the
/// network's latest tip. (In the case of the Hermes backend, it will
/// sleep for some time between polling for new tips; with the native
/// protocol backend, it will block waiting for the server to send us
/// new tip announcements.)
pub fn net_sync<A: Api>(
    net: &mut A,
    net_cfg: &net::Config,
    genesis_data: &GenesisData,
    storage: Arc<RwLock<Storage>>,
    sync_once: bool,
) -> Result<()> {
    // recover and print the TIP of the network
    let mut tip_header = net.get_tip()?;

    loop {
        storage
            .write()
            .expect("Failed to write net-tip into storage!")
            .net_tip = Some(tip_header.clone());

        net_sync_to(net, net_cfg, genesis_data, storage.clone(), &tip_header)?;

        if sync_once {
            break;
        }

        tip_header = net.wait_for_new_tip(&tip_header.compute_hash())?;
    }

    Ok(())
}

// Create an epoch from a complete set of previously fetched blocks on
// disk.
fn maybe_create_epoch(
    storage: &mut Storage,
    genesis_data: &GenesisData,
    epoch_id: EpochId,
    last_block: &HeaderHash,
) -> Result<()> {
    if epoch_exists(&storage.config, epoch_id).unwrap() {
        return Ok(());
    }

    info!("Packing epoch {}", epoch_id);

    let mut epoch_writer_state = EpochWriterState {
        epoch_id,
        writer: pack::packwriter_init(&storage.config).unwrap(),
        write_start_time: SystemTime::now(),
        blobs_to_delete: vec![],
    };

    let (end_of_prev_epoch, blocks, is_epoch_with_ebb) = get_unpacked_blocks_in_epoch(
        storage,
        last_block,
        epoch_writer_state.epoch_id,
        &mut epoch_writer_state.blobs_to_delete,
    );

    let mut chain_state =
        chain_state::restore_chain_state(storage, genesis_data, &end_of_prev_epoch)?;

    append_blocks_to_epoch_reverse(&mut epoch_writer_state, &mut chain_state, blocks)?;

    finish_epoch(
        storage,
        genesis_data,
        epoch_writer_state,
        &chain_state,
        is_epoch_with_ebb,
    )?;

    Ok(())
}

fn append_blocks_to_epoch_reverse(
    epoch_writer_state: &mut EpochWriterState,
    chain_state: &mut ChainState,
    mut blocks: Vec<(HeaderHash, RawBlock, Block)>,
) -> Result<()> {
    while let Some((hash, block_raw, block)) = blocks.pop() {
        chain_state.verify_block(&hash, &block)?;
        epoch_writer_state
            .writer
            .append(&types::header_to_blockhash(&hash), block_raw.as_ref())
            .unwrap();
    }

    Ok(())
}

fn get_unpacked_blocks_in_epoch(
    storage: &Storage,
    last_block: &HeaderHash,
    epoch_id: EpochId,
    blobs_to_delete: &mut Vec<HeaderHash>,
) -> (HeaderHash, Vec<(HeaderHash, RawBlock, Block)>, bool) {
    let mut cur_hash = last_block.clone();
    let mut blocks = vec![];
    let is_ebb_present: bool;
    loop {
        let block_raw = storage.read_block(&cur_hash.clone().into()).unwrap();
        blobs_to_delete.push(cur_hash.clone());
        let block = block_raw.decode().unwrap();
        let (blockdate, prev_hash) = {
            let hdr = block.header();
            (hdr.blockdate(), hdr.previous_header())
        };
        // Only add block and switch to previous block
        // if currently viewed block is still from the current epoch
        let from_this_epoch = blockdate.get_epochid() == epoch_id;
        if from_this_epoch {
            blocks.push((cur_hash, block_raw, block));
            cur_hash = prev_hash;
        }
        // Terminate if current block is EBB
        // or if there isn't one and we already jumped to previous epoch
        if blockdate.is_boundary() || !from_this_epoch {
            is_ebb_present = from_this_epoch && blockdate.is_boundary();
            break;
        }
    }
    (cur_hash, blocks, is_ebb_present)
}

fn finish_epoch(
    storage: &mut Storage,
    genesis_data: &GenesisData,
    epoch_writer_state: EpochWriterState,
    chain_state: &ChainState,
    is_epoch_with_ebb: bool,
) -> Result<()> {
    let epoch_id = epoch_writer_state.epoch_id;
    let (packhash, index) = pack::packwriter_finalize(&storage.config, epoch_writer_state.writer);
    let (lookup, tmpfile) = pack::create_index(&storage, &index);
    tmpfile.render_permanent(&storage.config.get_index_filepath(&packhash))?;
    storage.add_lookup(packhash, lookup);
    let epoch_time_elapsed = epoch_writer_state.write_start_time.elapsed().unwrap();

    if epoch_id > 0 {
        assert!(
            epoch_exists(&storage.config, epoch_id - 1)?,
            "Attempted finish_epoch() with non-existent previous epoch (ID {}, previous' ID {})",
            epoch_id,
            epoch_id - 1
        );
    }

    assert_eq!(chain_state.last_date.unwrap().get_epochid(), epoch_id);

    let epoch_flags = EpochFlags {
        is_ebb: is_epoch_with_ebb,
    };

    epoch::epoch_create(
        storage,
        &packhash,
        epoch_id,
        index,
        Some((chain_state, genesis_data)),
        &epoch_flags,
    );

    info!(
        "=> pack {} written for epoch {} in {}",
        hex::encode(&packhash[..]),
        epoch_id,
        duration_print(epoch_time_elapsed)
    );

    for hash in &epoch_writer_state.blobs_to_delete {
        debug!("removing blob {}", hash);
        blob::remove(&storage, &hash.clone().into());
    }

    let diff = storage
        .read_block(&types::header_to_blockhash(&chain_state.last_block))?
        .decode()?
        .header()
        .difficulty();

    // Drop this epoch from loose index
    storage.drop_loose_index_before(diff);

    Ok(())
}

pub fn get_peer(blockchain: &str, cfg: &net::Config, native: bool) -> Peer {
    for peer in cfg.peers.iter() {
        if (native && peer.is_native()) || (!native && peer.is_http()) {
            return Peer::new(
                String::from(blockchain),
                peer.name().to_owned(),
                peer.peer().clone(),
                cfg.protocol_magic,
            )
            .unwrap();
        }
    }

    panic!("no peer to connect to")
}

pub fn get_chain_state_at_end_of(
    storage: &Storage,
    epoch_id: EpochId,
    genesis_data: &GenesisData,
) -> Result<ChainState> {
    Ok(chain_state::read_chain_state(
        storage,
        genesis_data,
        &chain_state::get_last_block_of_epoch(storage, epoch_id)?,
    )?)
}
