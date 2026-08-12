package channelmembers

import "context"

// LiveMembership identifies one UID-owned membership candidate that must be
// confirmed against channel-owned subscriber metadata before data is exposed.
type LiveMembership struct {
	UID           string
	ChannelID     string
	ChannelType   int64
	SourceVersion uint64
}

// LiveMembershipAuthorityResult is aligned with one LiveMembership.
type LiveMembershipAuthorityResult struct {
	ChannelFound              bool
	Subscriber                bool
	Disband                   bool
	SubscriberMutationVersion uint64
	Err                       error
}

// LiveMembershipAuthority confirms non-person membership candidates through
// one uncached authoritative batch and repairs stale UID-owned projections.
type LiveMembershipAuthority interface {
	AuthorizeLiveMemberships(context.Context, []LiveMembership) []LiveMembershipAuthorityResult
	TombstoneRevokedMembership(context.Context, LiveMembership, uint64, int64) error
}
