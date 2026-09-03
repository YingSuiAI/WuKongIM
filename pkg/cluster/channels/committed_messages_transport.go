package channels

import (
	"context"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
)

func encodeCommittedMessagesRequest(req CommittedMessagesRequest) []byte {
	body := appendChannelID(nil, req.ChannelID)
	values := []uint64{
		req.AfterMessageSeq, uint64(req.Limit), req.ScanHead, uint64(req.ExpectedLeader),
		req.ExpectedChannelEpoch, req.ExpectedLeaderEpoch, uint64(req.ExpectedMinISR), req.ExpectedRetentionThroughSeq,
	}
	for _, value := range values {
		body = appendUvarint(body, value)
	}
	return encodeFrame(kindCommittedMessages, body)
}

func decodeCommittedMessagesRequest(data []byte) (CommittedMessagesRequest, error) {
	var req CommittedMessagesRequest
	version, body, err := decodeFrameWithVersion(data, kindCommittedMessages)
	if err != nil || version != codecVersion {
		return req, errInvalidCodecFrame
	}
	var offset int
	req.ChannelID, offset, err = readChannelID(body, 0)
	if err != nil {
		return CommittedMessagesRequest{}, err
	}
	values := [8]uint64{}
	for index := range values {
		values[index], offset, err = readUvarint(body, offset)
		if err != nil {
			return CommittedMessagesRequest{}, err
		}
	}
	if offset != len(body) || values[1] > uint64(maxInt()) || values[6] > uint64(maxInt()) {
		return CommittedMessagesRequest{}, errInvalidCodecFrame
	}
	req.AfterMessageSeq, req.Limit, req.ScanHead = values[0], int(values[1]), values[2]
	req.ExpectedLeader = ch.NodeID(values[3])
	req.ExpectedChannelEpoch, req.ExpectedLeaderEpoch = values[4], values[5]
	req.ExpectedMinISR, req.ExpectedRetentionThroughSeq = int(values[6]), values[7]
	return req, nil
}

func (c *TransportClient) ForwardCommittedMessages(ctx context.Context, node ch.NodeID, req CommittedMessagesRequest) (CommittedMessagesResult, error) {
	data, err := c.callShard(ctx, uint64(node), clusternet.RPCChannelCommittedMessages, channelForwardShardKey(req.ChannelID), encodeCommittedMessagesRequest(req))
	if err != nil {
		return CommittedMessagesResult{}, err
	}
	version, _, err := decodeFrameWithVersion(data, kindCommittedMessagesResponse)
	if err != nil || version != codecVersion {
		return CommittedMessagesResult{}, errInvalidCodecFrame
	}
	var response CommittedMessagesResult
	if err := decodeRPCResult(data, kindCommittedMessagesResponse, &response); err != nil {
		return CommittedMessagesResult{}, err
	}
	return response, nil
}

func (g *ServiceGateway) handleForwardCommittedMessages(ctx context.Context, req CommittedMessagesRequest) (CommittedMessagesResult, error) {
	service, err := g.service()
	if err != nil {
		return CommittedMessagesResult{}, err
	}
	return service.handleForwardCommittedMessages(ctx, req)
}

var _ committedMessagesForwarder = (*TransportClient)(nil)
