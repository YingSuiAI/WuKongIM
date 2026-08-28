package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	"github.com/WuKongIM/WuKongIM/pkg/controller/state"
	"github.com/WuKongIM/WuKongIM/pkg/controller/statefile"
	"github.com/WuKongIM/WuKongIM/pkg/db/message"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/raftlog"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func runUpgradeDirectory(ctx context.Context, flags cliFlags, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("upgrade-person-directory", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var nodeID uint64
	var clusterID string
	var dryRun bool
	fs.Uint64Var(&nodeID, "node-id", 0, "expected sole Slot and Controller voter ID")
	fs.StringVar(&clusterID, "cluster-id", "", "expected immutable cluster identity")
	fs.BoolVar(&dryRun, "dry-run", false, "validate identity, drained Slot boundaries and channel values without converting data")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	if flags.dataDir == "" || flags.configPath != "" || flags.metaPath != "" || flags.messagePath != "" || nodeID == 0 || clusterID == "" || len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "usage (stopped node, retain a full pre-upgrade backup): wkdb --data-dir DIR upgrade-person-directory --node-id ID --cluster-id ID")
		return exitConfig
	}
	if err := upgradeDirectory(ctx, flags.dataDir, nodeID, clusterID, dryRun, stdout); err != nil {
		fmt.Fprintf(stderr, "directory upgrade failed: %v\n", err)
		return exitInternal
	}
	return exitOK
}

// upgradeDirectory is deliberately a stopped single-node-cluster operation, not
// a mixed-version business path. The complete original directory is the rollback
// unit; never run the source binary on a partially or fully converted directory.
func upgradeDirectory(ctx context.Context, dir string, nodeID uint64, clusterID string, dryRun bool, out io.Writer) error {
	for _, name := range []string{"slotmeta", "slotraft", "controller", "messages"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("%s is not a directory", name)
		}
	}
	stateStore := statefile.New(filepath.Join(dir, "controller", "cluster-state.json"))
	control, err := stateStore.Load(ctx)
	if err != nil {
		return err
	}
	if err := validateDirectoryUpgradeOwner(control, nodeID, clusterID); err != nil {
		return err
	}
	// Pebble's exclusive locks reject a live node or another upgrader. Hold all
	// three stores before inspecting or changing any upgrade boundary.
	raftDB, err := raftlog.Open(filepath.Join(dir, "slotraft"), raftlog.Options{})
	if err != nil {
		return err
	}
	defer raftDB.Close()
	metaDB, err := metadb.Open(filepath.Join(dir, "slotmeta"))
	if err != nil {
		return err
	}
	defer metaDB.Close()
	messages, err := message.Open(filepath.Join(dir, "messages"))
	if err != nil {
		return err
	}
	defer messages.Close()
	lockedControl, err := stateStore.Load(ctx)
	if err != nil {
		return err
	}
	if control.Checksum != lockedControl.Checksum {
		return fmt.Errorf("controller changed while acquiring storage locks; stop the node first")
	}
	type slotUpgrade struct {
		id      uint32
		storage multiraft.Storage
		machine multiraft.StateMachine
	}
	plans := make([]slotUpgrade, 0, len(control.Slots))
	for _, assignment := range control.Slots {
		hashSlots := make([]uint16, 0)
		for _, r := range control.HashSlots.Ranges {
			if r.SlotID != assignment.SlotID {
				continue
			}
			for h := uint32(r.From); h <= uint32(r.To); h++ {
				hashSlots = append(hashSlots, uint16(h))
			}
		}
		machine, err := metafsm.NewStateMachineWithHashSlots(metaDB, uint64(assignment.SlotID), hashSlots)
		if err != nil {
			return err
		}
		storage := raftDB.ForSlot(uint64(assignment.SlotID))
		boundary, err := multiraft.OfflineSnapshotBoundary(ctx, storage, machine)
		if err != nil {
			return fmt.Errorf("slot %d: %w", assignment.SlotID, err)
		}
		conf := boundary.ConfState
		if len(conf.Voters) != 1 || conf.Voters[0] != nodeID || len(conf.Learners) != 0 || len(conf.VotersOutgoing) != 0 || len(conf.LearnersNext) != 0 || conf.AutoLeave {
			return fmt.Errorf("slot %d is not the expected stable single-voter group", assignment.SlotID)
		}
		plans = append(plans, slotUpgrade{assignment.SlotID, storage, machine})
		fmt.Fprintf(out, "preflight slot=%d applied=%d commit=%d last=%d\n", assignment.SlotID, boundary.AppliedIndex, boundary.HardState.Commit, boundary.HardState.Commit)
	}
	if err := metaDB.ValidateDirectoryProjectionUpgrade(ctx); err != nil {
		return err
	}
	resolve := func(entry message.ChannelCatalogEntry) (message.LegacyProposalAuthority, error) {
		hashSlot := routing.HashSlotForKey(entry.ID.ID, control.Config.HashSlotCount)
		meta, err := metaDB.ForHashSlot(hashSlot).GetChannelRuntimeMeta(ctx, entry.ID.ID, int64(entry.ID.Type))
		if err != nil {
			return message.LegacyProposalAuthority{}, err
		}
		if len(meta.Replicas) != 1 || meta.Replicas[0] != nodeID || len(meta.ISR) != 1 || meta.ISR[0] != nodeID || meta.Leader != nodeID || meta.MinISR != 1 || meta.WriteFenceToken != "" {
			return message.LegacyProposalAuthority{}, fmt.Errorf("channel %q/%d is not an unfenced local single-voter authority", entry.ID.ID, entry.ID.Type)
		}
		return message.LegacyProposalAuthority{ChannelEpoch: meta.ChannelEpoch, LeaderTerm: meta.LeaderEpoch, FenceVersion: meta.RouteGeneration, Leader: meta.Leader, Voters: meta.ISR}, nil
	}
	if _, err := messages.UpgradeLegacyProposals(ctx, nodeID, resolve, false); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if dryRun {
		fmt.Fprintln(out, "directory upgrade preflight complete; no business data converted")
		return nil
	}
	marker := filepath.Join(dir, metadb.DirectoryProjectionUpgradePendingFile)
	file, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		err = file.Sync()
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	} else if !os.IsExist(err) {
		return err
	} else if info, statErr := os.Lstat(marker); statErr != nil || !info.Mode().IsRegular() || info.Size() != 0 {
		return fmt.Errorf("invalid directory upgrade marker: %v", statErr)
	}
	if err := syncUpgradeDirectory(dir); err != nil {
		return err
	}
	converted, err := metaDB.UpgradeDirectoryProjection(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "converted_channel_rows=%d\n", converted)
	proofs, err := messages.UpgradeLegacyProposals(ctx, nodeID, resolve, true)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "message_channels_scanned=%d message_channels_upgraded=%d message_entries_upgraded=%d\n", proofs.ChannelsScanned, proofs.ChannelsUpgraded, proofs.EntriesUpgraded)
	for _, plan := range plans {
		if err := multiraft.ReplaceOfflineStateSnapshot(ctx, plan.storage, plan.machine); err != nil {
			return fmt.Errorf("slot %d snapshot: %w", plan.id, err)
		}
		fmt.Fprintf(out, "replaced_slot_snapshot=%d\n", plan.id)
	}
	if err := os.Remove(marker); err != nil {
		return err
	}
	if err := syncUpgradeDirectory(dir); err != nil {
		return err
	}
	fmt.Fprintln(out, "directory upgrade complete; start only the target binary")
	return nil
}

func validateDirectoryUpgradeOwner(control state.ClusterState, nodeID uint64, clusterID string) error {
	if control.ClusterID != clusterID || len(control.Controllers) != 1 || control.Controllers[0].NodeID != nodeID || control.Config.ReplicaCount != 1 || len(control.Slots) != int(control.Config.SlotCount) || len(control.Tasks) != 0 || control.SlotReplicaCountTransition != nil || control.ControllerVoterPromotion != nil {
		return fmt.Errorf("expected a converged single-node cluster with matching identity and no active topology task")
	}
	if control.ScheduledBackup != nil && (control.ScheduledBackup.ActiveBackup != nil || control.ScheduledBackup.ActiveRestore != nil) {
		return fmt.Errorf("backup or restore is active")
	}
	for _, slot := range control.Slots {
		if len(slot.DesiredPeers) != 1 || slot.DesiredPeers[0] != nodeID {
			return fmt.Errorf("slot %d is not owned solely by node %d", slot.SlotID, nodeID)
		}
	}
	return nil
}

func syncUpgradeDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
