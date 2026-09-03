package backup

import (
	"sort"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
)

// MissingDefinitions computes, per partition, the definitions present
// somewhere in the cluster but absent locally (mid-backup deploy skew).
func MissingDefinitions(perPartition map[uint32][]*proto.DefinitionRef) map[uint32][]*proto.DefinitionRef {
	type refKey struct {
		key int64
		typ proto.DefinitionType
	}
	union := map[refKey]*proto.DefinitionRef{}
	for _, refs := range perPartition {
		for _, r := range refs {
			union[refKey{r.GetKey(), r.GetType()}] = r
		}
	}
	// deterministic union ordering
	ordered := make([]*proto.DefinitionRef, 0, len(union))
	for _, r := range union {
		ordered = append(ordered, r)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].GetKey() < ordered[j].GetKey() })

	missing := map[uint32][]*proto.DefinitionRef{}
	for part, refs := range perPartition {
		have := map[refKey]bool{}
		for _, r := range refs {
			have[refKey{r.GetKey(), r.GetType()}] = true
		}
		for _, r := range ordered {
			if !have[refKey{r.GetKey(), r.GetType()}] {
				missing[part] = append(missing[part], r)
			}
		}
	}
	return missing
}

// PointerPlan holds the recomputed message_subscription_pointer placement after
// a cluster restore. ByPartition maps each home partition to the winning
// subscription rows that should be written there. Conflicts lists any
// (name, correlationKey) groups that had more than one active subscription.
type PointerPlan struct {
	ByPartition map[uint32][]*proto.MessageSubscriptionRow
	Conflicts   []PointerConflict
}

// PlanPointerRebuild recomputes the message_subscription_pointer placement for
// every ACTIVE subscription in the restored cluster. Duplicates on
// (name, correlationKey) — possible from snapshot skew — resolve to the newest
// CreatedAt (ties: highest Key); losers are reported in Conflicts, not silently
// dropped. Output ordering is deterministic (groups sorted by name then
// correlationKey).
func PlanPointerRebuild(subs []*proto.MessageSubscriptionRow, homePartition func(name, correlationKey string) uint32) PointerPlan {
	type slot struct{ name, ck string }
	groups := map[slot][]*proto.MessageSubscriptionRow{}
	for _, s := range subs {
		k := slot{s.GetName(), s.GetCorrelationKey()}
		groups[k] = append(groups[k], s)
	}

	plan := PointerPlan{ByPartition: map[uint32][]*proto.MessageSubscriptionRow{}}

	// deterministic iteration for stable output
	keys := make([]slot, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].ck < keys[j].ck
	})

	for _, k := range keys {
		rows := groups[k]
		// sort descending: newest CreatedAt first; tie-break by higher Key
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].GetCreatedAt() != rows[j].GetCreatedAt() {
				return rows[i].GetCreatedAt() > rows[j].GetCreatedAt()
			}
			return rows[i].GetKey() > rows[j].GetKey()
		})
		winner := rows[0]
		part := homePartition(k.name, k.ck)
		plan.ByPartition[part] = append(plan.ByPartition[part], winner)
		if len(rows) > 1 {
			c := PointerConflict{
				Name:           k.name,
				CorrelationKey: k.ck,
				WinnerKey:      winner.GetKey(),
			}
			for _, loser := range rows[1:] {
				c.LoserKeys = append(c.LoserKeys, loser.GetKey())
			}
			plan.Conflicts = append(plan.Conflicts, c)
		}
	}
	return plan
}
