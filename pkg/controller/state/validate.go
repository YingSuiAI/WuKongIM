package state

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
)

// Validate checks whether the cluster state satisfies durable Controller invariants.
func (s ClusterState) Validate() error {
	s = s.Clone()
	s.Normalize()
	if s.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("%w: %d", ErrUnsupportedSchema, s.SchemaVersion)
	}
	if s.ClusterID == "" {
		return invalid("cluster_id is required")
	}
	if s.Revision == 0 {
		return invalid("revision is required")
	}
	if s.Config.SlotCount == 0 || s.Config.HashSlotCount == 0 || s.Config.ReplicaCount == 0 {
		return invalid("slot_count, hash_slot_count, and replica_count must be positive")
	}
	if s.Config.SlotCount > uint32(s.Config.HashSlotCount) {
		return invalid("slot_count must not exceed hash_slot_count")
	}

	nodes, err := validateNodes(s.Nodes)
	if err != nil {
		return err
	}
	if err := validateControllers(s.Controllers, nodes); err != nil {
		return err
	}
	if promotion := s.ControllerVoterPromotion; promotion != nil {
		if s.SlotReplicaCountTransition != nil {
			return invalid("Controller voter promotion cannot overlap Slot replica count transition")
		}
		node, ok := nodes[promotion.TargetNodeID]
		if !ok || node.Addr != promotion.TargetAddr || node.JoinState != NodeJoinStateActive || !node.HasRole(NodeRoleData) {
			return invalid("Controller voter promotion requires the reserved active data node")
		}
		voters := make([]uint64, 0, len(s.Controllers))
		for _, voter := range s.Controllers {
			if voter.NodeID == promotion.TargetNodeID {
				return invalid("Controller voter promotion target is already a voter")
			}
			voters = append(voters, voter.NodeID)
		}
		if !equalUint64Set(voters, promotion.PreviousVoters) {
			return invalid("Controller voter promotion previous voters changed")
		}
	}
	if s.SlotReplicaCountTransition != nil {
		if len(s.Controllers) != 1 {
			return invalid("slot replica count transition requires exactly one Controller voter")
		}
		for _, nodeID := range s.SlotReplicaCountTransition.TargetNodeIDs {
			node, ok := nodes[nodeID]
			if !ok || node.JoinState != NodeJoinStateActive || !node.HasRole(NodeRoleData) {
				return invalid("slot replica count transition target must be an active data node")
			}
		}
	}
	if err := validateNodeHealthReports(s.NodeHealthReports, nodes); err != nil {
		return err
	}
	if err := validateSlotReplicaCountTransition(s.Config, s.SlotReplicaCountTransition); err != nil {
		return err
	}
	assignments, err := validateSlots(s.Config, s.SlotReplicaCountTransition, s.Slots, nodes)
	if err != nil {
		return err
	}
	if s.SlotReplicaCountTransition != nil && len(assignments) != int(s.Config.SlotCount) {
		return invalid("slot replica count transition requires every Slot assignment")
	}
	if err := validateHashSlots(s.Config, s.HashSlots); err != nil {
		return err
	}
	if err := validateTasks(s.Tasks, assignments, nodes, s.SlotReplicaCountTransition); err != nil {
		return err
	}
	if s.SlotReplicaCountTransition != nil {
		for _, task := range s.Tasks {
			if task.Kind != TaskKindSlotReplicaMove || task.SourceNode != 0 {
				return invalid("slot replica count transition permits only voter-add tasks")
			}
		}
	}
	if err := validateScheduledBackup(s.ScheduledBackup); err != nil {
		return err
	}
	if s.ScheduledBackup != nil && s.ScheduledBackup.ActiveRestore != nil {
		if len(s.Tasks) != 0 {
			return invalid("active restore requires an empty Controller task set")
		}
		if s.Config.HashSlotCount != BackupHashSlotCount {
			return invalid("active restore requires exactly 256 hash slots")
		}
	}
	if s.ScheduledBackup != nil && s.ScheduledBackup.ActiveBackup != nil &&
		len(s.Tasks) != 0 {
		return invalid("active backup requires an empty Controller task set")
	}
	if s.SlotReplicaCountTransition != nil && s.ScheduledBackup != nil &&
		(s.ScheduledBackup.ActiveBackup != nil || s.ScheduledBackup.ActiveRestore != nil) {
		return invalid("slot replica count transition cannot overlap backup or restore")
	}
	if err := validateOpsMCP(s.OpsMCP, nodes); err != nil {
		return err
	}
	return nil
}

func validateOpsMCP(opsMCP *OpsMCPState, nodes map[uint64]Node) error {
	if opsMCP == nil {
		return nil
	}
	if len(opsMCP.Credentials) > MaxOpsMCPCredentials {
		return invalid("ops_mcp credentials exceed limit")
	}
	if opsMCP.ProfileFenceUntilUnixMillis < 0 {
		return invalid("ops_mcp profile fence is invalid")
	}
	if opsMCP.Enabled && opsMCP.OwnerNodeID == 0 {
		return invalid("enabled ops_mcp requires an owner")
	}
	if opsMCP.Enabled && len(opsMCP.Credentials) == 0 {
		return invalid("enabled ops_mcp requires a credential")
	}
	if opsMCP.OwnerNodeID != 0 {
		owner, ok := nodes[opsMCP.OwnerNodeID]
		if !ok || owner.JoinState != NodeJoinStateActive {
			return invalid("ops_mcp owner must be an active cluster node")
		}
	}
	credentialIDs := make(map[string]struct{}, len(opsMCP.Credentials))
	for _, credential := range opsMCP.Credentials {
		if !validOpsMCPCredentialID(credential.ID) || !validSHA256(credential.DigestSHA256) || credential.CreatedAtUnixMillis <= 0 {
			return invalid("ops_mcp credential is invalid")
		}
		if _, exists := credentialIDs[credential.ID]; exists {
			return invalid("duplicate ops_mcp credential id")
		}
		credentialIDs[credential.ID] = struct{}{}
	}
	return nil
}

func validOpsMCPCredentialID(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func validateNodes(nodes []Node) (map[uint64]Node, error) {
	byID := make(map[uint64]Node, len(nodes))
	for _, node := range nodes {
		if node.NodeID == 0 {
			return nil, invalid("node_id must be non-zero")
		}
		if _, exists := byID[node.NodeID]; exists {
			return nil, invalid("duplicate node_id")
		}
		if node.Addr == "" {
			return nil, invalid("node addr is required")
		}
		if node.JoinState == "" {
			return nil, invalid("node join_state is required")
		}
		if node.JoinState != NodeJoinStateActive &&
			node.JoinState != NodeJoinStateJoining &&
			node.JoinState != NodeJoinStateLeaving &&
			node.JoinState != NodeJoinStateRemoved {
			return nil, invalid("unknown node join_state")
		}
		if node.Status == "" {
			return nil, invalid("node status is required")
		}
		if !validNodeStatus(node.Status) {
			return nil, invalid("unknown node status")
		}
		seenRoles := make(map[NodeRole]struct{}, len(node.Roles))
		for _, role := range node.Roles {
			if role != NodeRoleControllerVoter && role != NodeRoleData {
				return nil, invalid("unknown node role")
			}
			if _, exists := seenRoles[role]; exists {
				return nil, invalid("duplicate node role")
			}
			seenRoles[role] = struct{}{}
		}
		if len(seenRoles) == 0 {
			return nil, invalid("node roles are required")
		}
		byID[node.NodeID] = node
	}
	return byID, nil
}

func validateControllers(controllers []ControllerVoter, nodes map[uint64]Node) error {
	if len(controllers) == 0 {
		return invalid("controller voters are required")
	}
	seen := make(map[uint64]struct{}, len(controllers))
	for _, controller := range controllers {
		if controller.NodeID == 0 {
			return invalid("controller node_id must be non-zero")
		}
		if _, exists := seen[controller.NodeID]; exists {
			return invalid("duplicate controller node_id")
		}
		seen[controller.NodeID] = struct{}{}
		if controller.Addr == "" {
			return invalid("controller addr is required")
		}
		if controller.Role != ControllerRoleVoter {
			return invalid("controller role must be voter")
		}
		node, ok := nodes[controller.NodeID]
		if !ok || !node.HasRole(NodeRoleControllerVoter) || node.JoinState != NodeJoinStateActive {
			return invalid("controller voter must reference active controller_voter node")
		}
	}
	return nil
}

func validateSlotReplicaCountTransition(config ClusterConfig, transition *SlotReplicaCountTransition) error {
	if transition == nil {
		return nil
	}
	if transition.SourceReplicaCount != config.ReplicaCount ||
		transition.TargetReplicaCount <= transition.SourceReplicaCount ||
		transition.TargetReplicaCount != 3 || transition.StartedAtRevision == 0 ||
		len(transition.TargetNodeIDs) != int(transition.TargetReplicaCount) || hasDuplicateUint64(transition.TargetNodeIDs) {
		return invalid("slot replica count transition is invalid")
	}
	return nil
}

func validateSlots(config ClusterConfig, transition *SlotReplicaCountTransition, slots []SlotAssignment, nodes map[uint64]Node) (map[uint32]SlotAssignment, error) {
	byID := make(map[uint32]SlotAssignment, len(slots))
	for _, slot := range slots {
		if slot.SlotID == 0 || slot.SlotID > config.SlotCount {
			return nil, invalid("slot_id out of range")
		}
		if _, exists := byID[slot.SlotID]; exists {
			return nil, invalid("duplicate slot_id")
		}
		if slot.ConfigEpoch == 0 {
			return nil, invalid("slot config_epoch is required")
		}
		minReplicaCount := int(config.ReplicaCount)
		maxReplicaCount := minReplicaCount
		if transition != nil {
			maxReplicaCount = int(transition.TargetReplicaCount)
		}
		if len(slot.DesiredPeers) < minReplicaCount || len(slot.DesiredPeers) > maxReplicaCount {
			return nil, invalid("slot desired_peers must be within the active replica count transition")
		}
		seenPeers := make(map[uint64]struct{}, len(slot.DesiredPeers))
		for _, peerID := range slot.DesiredPeers {
			if peerID == 0 {
				return nil, invalid("slot peer must be non-zero")
			}
			if _, exists := seenPeers[peerID]; exists {
				return nil, invalid("duplicate slot peer")
			}
			seenPeers[peerID] = struct{}{}
			node, ok := nodes[peerID]
			if !ok || !slotDesiredPeerNodeAllowed(node) {
				return nil, invalid("slot peer must be an active or leaving data node")
			}
			if transition != nil && !containsUint64(transition.TargetNodeIDs, peerID) {
				return nil, invalid("slot peer must belong to replica count transition targets")
			}
		}
		if slot.PreferredLeader != 0 {
			if _, ok := seenPeers[slot.PreferredLeader]; !ok {
				return nil, invalid("preferred leader must be a desired peer")
			}
		}
		byID[slot.SlotID] = slot
	}
	return byID, nil
}

func slotDesiredPeerNodeAllowed(node Node) bool {
	return node.HasRole(NodeRoleData) &&
		(node.JoinState == NodeJoinStateActive || node.JoinState == NodeJoinStateLeaving)
}

func validateHashSlots(config ClusterConfig, table HashSlotTable) error {
	if table.Version != CurrentHashSlotTableVersion {
		return invalid("unsupported hash slot table version")
	}
	if table.SlotCount != config.HashSlotCount {
		return invalid("hash slot table slot_count must match config")
	}
	if len(table.Ranges) == 0 {
		return invalid("hash slot ranges are required")
	}
	expectedFrom := uint32(0)
	last := uint32(config.HashSlotCount) - 1
	for _, r := range table.Ranges {
		if r.SlotID == 0 || r.SlotID > config.SlotCount {
			return invalid("hash slot range target out of range")
		}
		if r.From > r.To {
			return invalid("hash slot range from must not exceed to")
		}
		if uint32(r.From) != expectedFrom {
			return invalid("hash slot ranges must be contiguous")
		}
		if uint32(r.To) > last {
			return invalid("hash slot range exceeds hash_slot_count")
		}
		expectedFrom = uint32(r.To) + 1
	}
	if expectedFrom != uint32(config.HashSlotCount) {
		return invalid("hash slot ranges must cover full hash_slot_count")
	}
	return nil
}

func validateTasks(tasks []ReconcileTask, assignments map[uint32]SlotAssignment, nodes map[uint64]Node, transition *SlotReplicaCountTransition) error {
	seenTaskIDs := make(map[string]struct{}, len(tasks))
	seenSlots := make(map[uint32]struct{}, len(tasks))
	for _, task := range tasks {
		if task.TaskID == "" {
			return invalid("task_id is required")
		}
		if _, exists := seenTaskIDs[task.TaskID]; exists {
			return invalid("duplicate task_id")
		}
		seenTaskIDs[task.TaskID] = struct{}{}
		if task.SlotID == 0 {
			return invalid("task slot_id is required")
		}
		if _, exists := seenSlots[task.SlotID]; exists {
			return invalid("only one active task per slot is allowed")
		}
		seenSlots[task.SlotID] = struct{}{}
		if task.Status != TaskStatusPending && task.Status != TaskStatusRunning && task.Status != TaskStatusFailed {
			return invalid("unknown task status")
		}
		if task.CompletionPolicy != TaskCompletionPolicySingleObserver && task.CompletionPolicy != TaskCompletionPolicyAllTargetPeers {
			return invalid("unknown task completion_policy")
		}
		if err := validateParticipantProgress(task); err != nil {
			return err
		}
		switch task.Kind {
		case TaskKindBootstrap:
			if task.Step != TaskStepCreateSlot {
				return invalid("bootstrap task step must be create_slot")
			}
			assignment, ok := assignments[task.SlotID]
			if !ok {
				return invalid("bootstrap task requires slot assignment")
			}
			if !reflect.DeepEqual(task.TargetPeers, assignment.DesiredPeers) {
				return invalid("bootstrap target peers must match assignment")
			}
			if task.ConfigEpoch != assignment.ConfigEpoch {
				return invalid("bootstrap config_epoch must match assignment")
			}
			if task.TargetNode != assignment.PreferredLeader {
				return invalid("bootstrap target node must match preferred leader")
			}
		case TaskKindLeaderTransfer:
			if task.Step != TaskStepTransferLeader {
				return invalid("leader transfer task step must be transfer_leader")
			}
			assignment, ok := assignments[task.SlotID]
			if !ok {
				return invalid("leader transfer task requires slot assignment")
			}
			if task.SourceNode == 0 || task.TargetNode == 0 {
				return invalid("leader transfer source and target must be non-zero")
			}
			if task.SourceNode == task.TargetNode {
				return invalid("leader transfer source and target must differ")
			}
			if !containsUint64(assignment.DesiredPeers, task.SourceNode) {
				return invalid("leader transfer source must be a desired peer")
			}
			if !containsUint64(assignment.DesiredPeers, task.TargetNode) {
				return invalid("leader transfer target must be a desired peer")
			}
			if !reflect.DeepEqual(task.TargetPeers, assignment.DesiredPeers) {
				return invalid("leader transfer target peers must match assignment")
			}
			if task.ConfigEpoch != assignment.ConfigEpoch {
				return invalid("leader transfer config_epoch must match assignment")
			}
			if task.TargetNode != assignment.PreferredLeader {
				return invalid("leader transfer target node must match preferred leader")
			}
			if task.CompletionPolicy != TaskCompletionPolicySingleObserver {
				return invalid("leader transfer completion_policy must be single_observer")
			}
			if len(task.ParticipantProgress) != 0 {
				return invalid("leader transfer task must not have participant progress")
			}
		case TaskKindSlotReplicaMove:
			if task.Step != TaskStepOpenLearner &&
				task.Step != TaskStepAddLearner &&
				task.Step != TaskStepPromoteLearner &&
				task.Step != TaskStepRemoveVoter &&
				task.Step != TaskStepCommitAssignment {
				return invalid("slot replica move task step is invalid")
			}
			assignment, ok := assignments[task.SlotID]
			if !ok {
				return invalid("slot replica move task requires slot assignment")
			}
			if task.TargetNode == 0 {
				return invalid("slot replica move target must be non-zero")
			}
			if task.SourceNode != 0 && task.SourceNode == task.TargetNode {
				return invalid("slot replica move source and target must differ")
			}
			if task.ConfigEpoch != assignment.ConfigEpoch {
				return invalid("slot replica move config_epoch must match assignment")
			}
			if task.SourceNode != 0 && !containsUint64(assignment.DesiredPeers, task.SourceNode) {
				return invalid("slot replica move source must be a desired peer")
			}
			if containsUint64(assignment.DesiredPeers, task.TargetNode) {
				return invalid("slot replica move target must not already be a desired peer")
			}
			target, ok := nodes[task.TargetNode]
			if !ok || target.JoinState != NodeJoinStateActive || !target.HasRole(NodeRoleData) {
				return invalid("slot replica move target must be an active data node")
			}
			if task.SourceNode == 0 {
				if transition == nil || len(task.TargetPeers) != len(assignment.DesiredPeers)+1 ||
					len(task.TargetPeers) > int(transition.TargetReplicaCount) ||
					!equalUint64Set(task.TargetPeers, append(append([]uint64(nil), assignment.DesiredPeers...), task.TargetNode)) {
					return invalid("slot replica add target peers must append target")
				}
			} else if !reflect.DeepEqual(task.TargetPeers, replacePeer(assignment.DesiredPeers, task.SourceNode, task.TargetNode)) {
				return invalid("slot replica move target peers must replace source with target")
			}
			if task.CompletionPolicy != TaskCompletionPolicySingleObserver {
				return invalid("slot replica move completion_policy must be single_observer")
			}
			if len(task.ParticipantProgress) != 0 {
				return invalid("slot replica move task must not have participant progress")
			}
			if hasDuplicateUint64(task.ObservedVoters) || hasDuplicateUint64(task.ObservedLearners) {
				return invalid("slot replica move observed sets must be unique")
			}
		default:
			return invalid("unknown task kind")
		}
	}
	return nil
}

func validateNodeHealthReports(reports []NodeHealthReport, nodes map[uint64]Node) error {
	seen := make(map[uint64]struct{}, len(reports))
	for _, report := range reports {
		if report.NodeID == 0 {
			return invalid("node health report node_id must be non-zero")
		}
		if _, exists := seen[report.NodeID]; exists {
			return invalid("duplicate node health report")
		}
		seen[report.NodeID] = struct{}{}
		if _, ok := nodes[report.NodeID]; !ok {
			return invalid("node health report must reference a node")
		}
		if !validNodeStatus(report.Status) {
			return invalid("unknown node health report status")
		}
		if report.ReportedAtUnixMilli < 0 {
			return invalid("node health report reported_at_unix_milli must not be negative")
		}
		if len(report.ErrorCode) > 128 {
			return invalid("node health report error_code is too long")
		}
	}
	return nil
}

func validNodeStatus(status NodeStatus) bool {
	return status == NodeStatusAlive || status == NodeStatusSuspect || status == NodeStatusDown
}

func containsUint64(items []uint64, want uint64) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func equalUint64Set(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]uint64(nil), left...)
	right = append([]uint64(nil), right...)
	sort.Slice(left, func(i, j int) bool { return left[i] < left[j] })
	sort.Slice(right, func(i, j int) bool { return right[i] < right[j] })
	return reflect.DeepEqual(left, right)
}

func replacePeer(peers []uint64, source uint64, target uint64) []uint64 {
	out := append([]uint64(nil), peers...)
	for i, peer := range out {
		if peer == source {
			out[i] = target
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func hasDuplicateUint64(items []uint64) bool {
	seen := make(map[uint64]struct{}, len(items))
	for _, item := range items {
		if item == 0 {
			return true
		}
		if _, exists := seen[item]; exists {
			return true
		}
		seen[item] = struct{}{}
	}
	return false
}

func validateParticipantProgress(task ReconcileTask) error {
	if task.CompletionPolicy == TaskCompletionPolicySingleObserver {
		if len(task.ParticipantProgress) != 0 {
			return invalid("single_observer task must not have participant progress")
		}
		return nil
	}
	if task.CompletionPolicy != TaskCompletionPolicyAllTargetPeers {
		return invalid("unknown task completion_policy")
	}
	if len(task.ParticipantProgress) != len(task.TargetPeers) {
		return invalid("all_target_peers task progress must match target peers")
	}
	targets := make(map[uint64]struct{}, len(task.TargetPeers))
	for _, peerID := range task.TargetPeers {
		targets[peerID] = struct{}{}
	}
	seen := make(map[uint64]struct{}, len(task.ParticipantProgress))
	for _, progress := range task.ParticipantProgress {
		if progress.NodeID == 0 {
			return invalid("task participant node_id must be non-zero")
		}
		if _, ok := targets[progress.NodeID]; !ok {
			return invalid("task participant must be a target peer")
		}
		if _, exists := seen[progress.NodeID]; exists {
			return invalid("duplicate task participant")
		}
		seen[progress.NodeID] = struct{}{}
		switch progress.Status {
		case TaskParticipantStatusPending, TaskParticipantStatusDone, TaskParticipantStatusFailed:
		default:
			return invalid("unknown task participant status")
		}
	}
	return nil
}

func invalid(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidState, reason)
}
