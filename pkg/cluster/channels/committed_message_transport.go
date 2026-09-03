package channels

import (
	"context"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
)

func encodeCommittedMessageRequest(req CommittedMessageRequest) []byte {
	body := appendChannelID(nil, req.ChannelID)
	body = appendUvarint(body, req.MessageID)
	body = appendUvarint(body, req.MessageSeq)
	body = appendUvarint(body, uint64(req.ExpectedLeader))
	body = appendUvarint(body, req.ExpectedChannelEpoch)
	body = appendUvarint(body, req.ExpectedLeaderEpoch)
	body = appendUvarint(body, uint64(req.ExpectedMinISR))
	return encodeFrame(kindCommittedMessage, body)
}

func decodeCommittedMessageRequest(data []byte) (CommittedMessageRequest, error) {
	var req CommittedMessageRequest
	version, body, err := decodeFrameWithVersion(data, kindCommittedMessage)
	if err != nil || version != codecVersion {
		return req, errInvalidCodecFrame
	}
	var offset int
	req.ChannelID, offset, err = readChannelID(body, 0)
	if err != nil {
		return CommittedMessageRequest{}, err
	}
	values := [6]uint64{}
	for index := range values {
		values[index], offset, err = readUvarint(body, offset)
		if err != nil {
			return CommittedMessageRequest{}, err
		}
	}
	if offset != len(body) || values[5] > uint64(maxInt()) {
		return CommittedMessageRequest{}, errInvalidCodecFrame
	}
	req.MessageID, req.MessageSeq = values[0], values[1]
	req.ExpectedLeader = ch.NodeID(values[2])
	req.ExpectedChannelEpoch, req.ExpectedLeaderEpoch, req.ExpectedMinISR = values[3], values[4], int(values[5])
	return req, nil
}

// ForwardCommittedMessage uses only the dedicated exact-proof RPC.
func (c *TransportClient) ForwardCommittedMessage(ctx context.Context, node ch.NodeID, req CommittedMessageRequest) (CommittedMessageResult, error) {
	data, err := c.callShard(ctx, uint64(node), clusternet.RPCChannelCommittedMessage, channelForwardShardKey(req.ChannelID), encodeCommittedMessageRequest(req))
	if err != nil {
		return CommittedMessageResult{}, err
	}
	version, _, err := decodeFrameWithVersion(data, kindCommittedMessageResponse)
	if err != nil || version != codecVersion {
		return CommittedMessageResult{}, errInvalidCodecFrame
	}
	var response CommittedMessageResult
	if err := decodeRPCResult(data, kindCommittedMessageResponse, &response); err != nil {
		return CommittedMessageResult{}, err
	}
	return response, nil
}

func (g *ServiceGateway) handleForwardCommittedMessage(ctx context.Context, req CommittedMessageRequest) (CommittedMessageResult, error) {
	service, err := g.service()
	if err != nil {
		return CommittedMessageResult{}, err
	}
	return service.handleForwardCommittedMessage(ctx, req)
}

var _ committedMessageForwarder = (*TransportClient)(nil)
