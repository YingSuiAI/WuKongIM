package fsm

import (
	"encoding/binary"
	"fmt"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

const (
	tagPlatformRejoinUID                uint8 = 1
	tagPlatformRejoinChannelID          uint8 = 2
	tagPlatformRejoinChannelType        uint8 = 3
	tagPlatformRejoinEpoch              uint8 = 4
	tagPlatformRejoinJoinedSeq          uint8 = 5
	tagPlatformRejoinPreviousRemovedSeq uint8 = 6
	tagPlatformRejoinSourceVersion      uint8 = 7
	tagPlatformRejoinUpdatedAt          uint8 = 8
	tagPlatformRejoinRepairSameEpoch    uint8 = 9
)

type rejoinUserChannelMembershipCmd struct {
	rejoin metadb.PlatformMembershipRejoin
	result *metadb.PlatformMembershipRejoinResult
}

func (c *rejoinUserChannelMembershipCmd) apply(wb *metadb.WriteBatch, hashSlot uint16) error {
	result, err := wb.RejoinUserChannelMembership(hashSlot, c.rejoin)
	if err != nil {
		return err
	}
	c.result = result
	return nil
}

func (c *rejoinUserChannelMembershipCmd) applyResult() []byte {
	if c.result != nil && c.result.Accepted {
		return []byte(ApplyResultOK)
	}
	return []byte(ApplyResultMembershipEpochConflict)
}

// IsRejoinUserChannelMembershipCommand identifies the new typed command, so
// cluster callers can map its deterministic guard result to a visible error.
func IsRejoinUserChannelMembershipCommand(data []byte) bool {
	return len(data) >= headerSize && data[0] == commandVersion && data[1] == cmdTypeRejoinUserChannelMembership
}

// EncodeRejoinUserChannelMembershipCommandChecked encodes one guarded Platform
// history epoch. Older Slot binaries reject this distinct command type.
func EncodeRejoinUserChannelMembershipCommandChecked(rejoin metadb.PlatformMembershipRejoin) ([]byte, error) {
	if err := ValidateSubscriberCommandLimits([]string{rejoin.UID}); err != nil {
		return nil, err
	}
	if rejoin.ChannelID == "" || rejoin.MembershipEpoch == 0 || rejoin.JoinedSeq == ^uint64(0) || rejoin.PreviousRemovedSeq > rejoin.JoinedSeq || rejoin.SourceVersion == 0 || rejoin.UpdatedAt < 0 {
		return nil, metadb.ErrInvalidArgument
	}
	buf := []byte{commandVersion, cmdTypeRejoinUserChannelMembership}
	buf = appendStringTLVField(buf, tagPlatformRejoinUID, rejoin.UID)
	buf = appendStringTLVField(buf, tagPlatformRejoinChannelID, rejoin.ChannelID)
	buf = appendInt64TLVField(buf, tagPlatformRejoinChannelType, rejoin.ChannelType)
	buf = appendUint64TLVField(buf, tagPlatformRejoinEpoch, rejoin.MembershipEpoch)
	buf = appendUint64TLVField(buf, tagPlatformRejoinJoinedSeq, rejoin.JoinedSeq)
	buf = appendUint64TLVField(buf, tagPlatformRejoinPreviousRemovedSeq, rejoin.PreviousRemovedSeq)
	buf = appendUint64TLVField(buf, tagPlatformRejoinSourceVersion, rejoin.SourceVersion)
	buf = appendInt64TLVField(buf, tagPlatformRejoinUpdatedAt, rejoin.UpdatedAt)
	if rejoin.RepairSameEpoch {
		buf = appendUint64TLVField(buf, tagPlatformRejoinRepairSameEpoch, 1)
	}
	return buf, nil
}

func decodeRejoinUserChannelMembership(data []byte) (command, error) {
	var rejoin metadb.PlatformMembershipRejoin
	var seen uint16
	for off := 0; off < len(data); {
		tag, value, n, err := readTLV(data[off:])
		if err != nil {
			return nil, err
		}
		off += n
		if tag < tagPlatformRejoinUID || tag > tagPlatformRejoinRepairSameEpoch {
			continue
		}
		bit := uint16(1) << (tag - 1)
		if seen&bit != 0 {
			return nil, fmt.Errorf("%w: duplicate Platform rejoin field", metadb.ErrCorruptValue)
		}
		seen |= bit
		switch tag {
		case tagPlatformRejoinUID:
			rejoin.UID = string(value)
		case tagPlatformRejoinChannelID:
			rejoin.ChannelID = string(value)
		case tagPlatformRejoinChannelType, tagPlatformRejoinEpoch, tagPlatformRejoinJoinedSeq, tagPlatformRejoinPreviousRemovedSeq, tagPlatformRejoinSourceVersion, tagPlatformRejoinUpdatedAt, tagPlatformRejoinRepairSameEpoch:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: bad Platform rejoin field length", metadb.ErrCorruptValue)
			}
			num := binary.BigEndian.Uint64(value)
			switch tag {
			case tagPlatformRejoinChannelType:
				rejoin.ChannelType = int64(num)
			case tagPlatformRejoinEpoch:
				rejoin.MembershipEpoch = num
			case tagPlatformRejoinJoinedSeq:
				rejoin.JoinedSeq = num
			case tagPlatformRejoinPreviousRemovedSeq:
				rejoin.PreviousRemovedSeq = num
			case tagPlatformRejoinSourceVersion:
				rejoin.SourceVersion = num
			case tagPlatformRejoinUpdatedAt:
				rejoin.UpdatedAt = int64(num)
			case tagPlatformRejoinRepairSameEpoch:
				if num != 1 {
					return nil, fmt.Errorf("%w: invalid Platform repair flag", metadb.ErrCorruptValue)
				}
				rejoin.RepairSameEpoch = true
			}
		}
	}
	if seen&0xff != 0xff {
		return nil, fmt.Errorf("%w: incomplete Platform rejoin command", metadb.ErrCorruptValue)
	}
	if rejoin.UID == "" || rejoin.ChannelID == "" || rejoin.MembershipEpoch == 0 || rejoin.JoinedSeq == ^uint64(0) || rejoin.PreviousRemovedSeq > rejoin.JoinedSeq || rejoin.SourceVersion == 0 || rejoin.UpdatedAt < 0 {
		return nil, fmt.Errorf("%w: invalid Platform rejoin command", metadb.ErrCorruptValue)
	}
	return &rejoinUserChannelMembershipCmd{rejoin: rejoin}, nil
}
