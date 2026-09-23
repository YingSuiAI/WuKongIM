package fsm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

const tagRefreshLargeThreshold uint8 = 3

var channelLargeRefreshResultMagic = [...]byte{'W', 'K', 'C', 'L', 1}

type refreshChannelLargeCmd struct {
	channelID   string
	channelType int64
	threshold   uint64
	result      *metadb.ChannelLargeRefreshResult
}

func (c *refreshChannelLargeCmd) apply(wb *metadb.WriteBatch, hashSlot uint16) error {
	result, err := wb.RefreshChannelLarge(hashSlot, c.channelID, c.channelType, c.threshold)
	c.result = result
	return err
}

func (c *refreshChannelLargeCmd) applyResult() []byte {
	// Channel contains only scalar fields, so JSON marshaling cannot fail.
	data, _ := json.Marshal(c.result)
	return append(append([]byte(nil), channelLargeRefreshResultMagic[:]...), data...)
}

// EncodeRefreshChannelLargeCommand computes Large from the current durable
// subscriber count when the command is applied in the Channel Slot.
func EncodeRefreshChannelLargeCommand(channelID string, channelType int64, threshold uint64) []byte {
	buf := make([]byte, headerSize+tlvOverhead+len(channelID)+tlvOverhead+8+tlvOverhead+8)
	buf[0], buf[1] = commandVersion, cmdTypeRefreshChannelLarge
	off := headerSize
	off = putStringField(buf, off, tagChannelID, channelID)
	off = putInt64Field(buf, off, tagChannelType, channelType)
	_ = putInt64Field(buf, off, tagRefreshLargeThreshold, int64(threshold))
	return buf
}

func decodeRefreshChannelLarge(data []byte) (command, error) {
	cmd := &refreshChannelLargeCmd{}
	var idSeen, typeSeen, thresholdSeen bool
	for len(data) > 0 {
		tag, value, used, err := readTLV(data)
		if err != nil {
			return nil, err
		}
		data = data[used:]
		switch tag {
		case tagChannelID:
			if idSeen {
				return nil, fmt.Errorf("%w: duplicate ChannelID", metadb.ErrCorruptValue)
			}
			idSeen = true
			cmd.channelID = string(value)
		case tagChannelType:
			if typeSeen || len(value) != 8 {
				return nil, fmt.Errorf("%w: bad ChannelType", metadb.ErrCorruptValue)
			}
			typeSeen = true
			cmd.channelType = int64(binary.BigEndian.Uint64(value))
		case tagRefreshLargeThreshold:
			if thresholdSeen || len(value) != 8 {
				return nil, fmt.Errorf("%w: bad threshold", metadb.ErrCorruptValue)
			}
			thresholdSeen = true
			cmd.threshold = binary.BigEndian.Uint64(value)
		}
	}
	if !idSeen || !typeSeen || !thresholdSeen || cmd.channelID == "" {
		return nil, fmt.Errorf("%w: incomplete Large refresh command", metadb.ErrCorruptValue)
	}
	return cmd, nil
}

// DecodeChannelLargeRefreshResult returns the complete Channel row observed
// and updated by the Slot command. Found is false for an absent Channel.
func DecodeChannelLargeRefreshResult(data []byte) (metadb.ChannelLargeRefreshResult, error) {
	if !bytes.HasPrefix(data, channelLargeRefreshResultMagic[:]) {
		return metadb.ChannelLargeRefreshResult{}, fmt.Errorf("%w: Large refresh result", metadb.ErrCorruptValue)
	}
	var result metadb.ChannelLargeRefreshResult
	if err := json.Unmarshal(data[len(channelLargeRefreshResultMagic):], &result); err != nil {
		return metadb.ChannelLargeRefreshResult{}, fmt.Errorf("%w: Large refresh result: %v", metadb.ErrCorruptValue, err)
	}
	return result, nil
}
