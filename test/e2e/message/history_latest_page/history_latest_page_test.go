//go:build e2e

package history_latest_page

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/test/e2e/suite"
	"github.com/stretchr/testify/require"
)

func TestLatestHistoryPageKeepsRecentMessagesAcrossAllIngresses(t *testing.T) {
	opts := []suite.Option{}
	for node := uint64(1); node <= 3; node++ {
		opts = append(opts, suite.WithNodeConfigOverrides(node, map[string]string{
			"WK_CLUSTER_HASH_SLOT_COUNT": "256",
		}))
	}
	cluster := suite.New(t).StartThreeNodeCluster(opts...)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, cluster.WaitClusterReady(ctx), cluster.DumpDiagnostics())
	const channel, sender, viewer = "latest-history-room", "latest-history-sender", "latest-history-viewer"
	require.NoError(t, suite.PostChannel(ctx, cluster.MustNode(1).APIAddr(), map[string]any{
		"channel_id": channel, "channel_type": 2, "reset": 1, "subscribers": []string{sender, viewer},
	}), cluster.DumpDiagnostics())
	receipts := make([]suite.MessageSendResponse, 0, 6)
	for index := 0; index < 6; index++ {
		receipt, err := suite.PostMessageSend(ctx, cluster.MustNode(uint64(index%3+1)).APIAddr(), map[string]any{
			"from_uid": sender, "channel_id": channel, "channel_type": 2,
			"client_msg_no": fmt.Sprintf("latest-history-%d", index),
			"payload":       base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("history body %d", index))),
		})
		require.NoError(t, err, cluster.DumpDiagnostics())
		require.Equal(t, uint8(1), receipt.Reason)
		receipts = append(receipts, receipt)
	}
	for _, node := range cluster.Nodes {
		t.Run(fmt.Sprintf("ingress_%d", node.Spec.ID), func(t *testing.T) {
			// Zero boundaries mean latest visible page, not sequence zero
			// clamped upward into a forward scan from the first visible row.
			for _, test := range []struct {
				name       string
				start, end uint64
				pull       int
				want       []suite.MessageSendResponse
			}{
				{"latest_down", 0, 0, 0, receipts[4:]},
				{"latest_up", 0, 0, 1, receipts[4:]},
				{"older", receipts[3].MessageSeq, 0, 0, receipts[2:4]},
				{"explicit_forward", receipts[2].MessageSeq, 0, 1, receipts[2:4]},
			} {
				t.Run(test.name, func(t *testing.T) {
					var page struct {
						Messages []struct {
							ID  int64  `json:"message_id"`
							Seq uint64 `json:"message_seq"`
						} `json:"messages"`
					}
					_, err := suite.PostJSON(ctx, "http://"+node.APIAddr()+"/channel/messagesync", map[string]any{
						"login_uid": viewer, "channel_id": channel, "channel_type": 2,
						"start_message_seq": test.start, "end_message_seq": test.end,
						"limit": 2, "pull_mode": test.pull,
					}, &page)
					require.NoError(t, err, node.DumpDiagnostics())
					require.Len(t, page.Messages, len(test.want))
					for index, message := range page.Messages {
						require.Equal(t, test.want[index].MessageSeq, message.Seq, "history selected wrong page")
						require.Equal(t, test.want[index].MessageID, message.ID, "history changed committed identity")
					}
				})
			}
		})
	}
}
