package channels

import (
	"context"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
)

type committedHeadResponse struct{ Sequence uint64 }

func encodeCommittedHeadRequest(req CommittedHeadRequest) []byte {
	body := appendChannelID(nil, req.ChannelID)
	body = appendUvarint(body, uint64(req.ExpectedLeader))
	body = appendUvarint(body, req.ExpectedChannelEpoch)
	body = appendUvarint(body, req.ExpectedLeaderEpoch)
	body = appendUvarint(body, uint64(req.ExpectedMinISR))
	return encodeFrame(kindCommittedHead, body)
}

func decodeCommittedHeadRequest(data []byte) (CommittedHeadRequest, error) {
	var req CommittedHeadRequest
	version, body, err := decodeFrameWithVersion(data, kindCommittedHead)
	if err != nil || version != codecVersion {
		return req, errInvalidCodecFrame
	}
	var offset int
	req.ChannelID, offset, err = readChannelID(body, 0)
	if err != nil {
		return CommittedHeadRequest{}, err
	}
	values := [4]uint64{}
	for index := range values {
		values[index], offset, err = readUvarint(body, offset)
		if err != nil {
			return CommittedHeadRequest{}, err
		}
	}
	if offset != len(body) || values[3] > uint64(maxInt()) {
		return CommittedHeadRequest{}, errInvalidCodecFrame
	}
	req.ExpectedLeader, req.ExpectedChannelEpoch, req.ExpectedLeaderEpoch, req.ExpectedMinISR = ch.NodeID(values[0]), values[1], values[2], int(values[3])
	return req, nil
}

// ForwardCommittedHead uses its own RPC capability and never falls back to a
// content-bearing head read or a legacy peer with no metadata-only endpoint.
func (c *TransportClient) ForwardCommittedHead(ctx context.Context, node ch.NodeID, req CommittedHeadRequest) (uint64, error) {
	data, err := c.callShard(ctx, uint64(node), clusternet.RPCChannelCommittedHead, channelForwardShardKey(req.ChannelID), encodeCommittedHeadRequest(req))
	if err != nil {
		return 0, err
	}
	version, _, err := decodeFrameWithVersion(data, kindCommittedHeadResponse)
	if err != nil || version != codecVersion {
		return 0, errInvalidCodecFrame
	}
	var response committedHeadResponse
	if err := decodeRPCResult(data, kindCommittedHeadResponse, &response); err != nil {
		return 0, err
	}
	return response.Sequence, nil
}

func (g *ServiceGateway) handleForwardCommittedHead(ctx context.Context, req CommittedHeadRequest) (uint64, error) {
	service, err := g.service()
	if err != nil {
		return 0, err
	}
	return service.handleForwardCommittedHead(ctx, req)
}

var _ committedHeadForwarder = (*TransportClient)(nil)
