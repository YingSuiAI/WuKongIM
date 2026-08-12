# internal/contracts/channelmembers Flow

## Responsibility

`internal/contracts/channelmembers` contains the stable member-list channel-id
namespace shared by channel management usecases and runtime adapters.

It must remain dependency-light and must not import access, app, gateway,
cluster, or storage packages. The generated IDs intentionally match the legacy
namespace so internal can read and write compatible allowlist, denylist, and
temporary subscriber rows.

`LiveMembershipAuthority` is the shared pull/conversation safety seam. For
every non-person live UID-owned membership candidate it returns aligned facts
from one uncached authoritative batch containing both Channel metadata and the
subscriber point lookup. A missing subscriber fails closed before message or
Channel-head reads. Callers may repair the stale UID projection only when the
authoritative `SubscriberMutationVersion` is strictly newer than the
candidate's `SourceVersion`; the authoritative version itself is the tombstone
fence, so an equal observation never invents `source+1`.
