package backup

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/stretchr/testify/assert"
)

func row(key int64, name, ck string, createdAt int64) *proto.MessageSubscriptionRow {
	return &proto.MessageSubscriptionRow{
		Key:            ptr.To(key),
		Name:           ptr.To(name),
		CorrelationKey: ptr.To(ck),
		CreatedAt:      ptr.To(createdAt),
		State:          ptr.To(int64(2)),
	}
}

func TestPlanPointerRebuild(t *testing.T) {
	// deterministic fake hash: route by first byte of correlationKey (or name)
	home := func(name, ck string) uint32 {
		s := ck
		if s == "" {
			s = name
		}
		return uint32(s[0]%2) + 1
	}
	tests := []struct {
		name          string
		subs          []*proto.MessageSubscriptionRow
		wantPerPart   map[uint32][]int64 // partition -> expected pointer subscription keys
		wantConflicts int
	}{
		{
			name: "routes by correlation key hash",
			subs: []*proto.MessageSubscriptionRow{row(10, "msg", "a", 1), row(11, "msg", "b", 1)},
			wantPerPart: map[uint32][]int64{
				home("msg", "a"): {10},
				home("msg", "b"): {11},
			},
		},
		{
			name: "definition-level subscription routes by name",
			subs: []*proto.MessageSubscriptionRow{row(20, "start-msg", "", 5)},
			wantPerPart: map[uint32][]int64{
				home("start-msg", ""): {20},
			},
		},
		{
			name:          "duplicate actives: newest created_at wins",
			subs:          []*proto.MessageSubscriptionRow{row(30, "msg", "same", 100), row(31, "msg", "same", 200)},
			wantPerPart:   map[uint32][]int64{home("msg", "same"): {31}},
			wantConflicts: 1,
		},
		{
			name:          "created_at tie: higher key wins deterministically",
			subs:          []*proto.MessageSubscriptionRow{row(41, "msg", "same", 100), row(40, "msg", "same", 100)},
			wantPerPart:   map[uint32][]int64{home("msg", "same"): {41}},
			wantConflicts: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanPointerRebuild(tt.subs, home)
			got := map[uint32][]int64{}
			for part, rows := range plan.ByPartition {
				for _, r := range rows {
					got[part] = append(got[part], r.GetKey())
				}
			}
			assert.Equal(t, tt.wantPerPart, got)
			assert.Len(t, plan.Conflicts, tt.wantConflicts)
			if tt.wantConflicts == 1 {
				c := plan.Conflicts[0]
				assert.Equal(t, "msg", c.Name)
				assert.Len(t, c.LoserKeys, 1)
			}
		})
	}
}
