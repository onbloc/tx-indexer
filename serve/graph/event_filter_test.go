package graph

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gnolang/tx-indexer/serve/graph/model"
)

func ptr[T any](v T) *T { return &v }

func TestFilteredEventBy(t *testing.T) {
	t.Parallel()

	gnoEvent := &model.GnoEvent{
		Type:    "Transfer",
		PkgPath: "gno.land/r/demo/foo",
		Attrs:   []*model.GnoEventAttribute{{Key: "from", Value: "g1from"}},
	}
	depositEvent := &model.StorageDepositEvent{
		Type:       "StorageDepositEvent",
		BytesDelta: 100,
		FeeDelta:   &model.Coin{Amount: 1000, Denom: "ugnot"},
		PkgPath:    "gno.land/r/demo/foo",
	}
	unlockEvent := &model.StorageUnlockEvent{
		Type:           "StorageUnlockEvent",
		BytesDelta:     -100,
		FeeRefund:      &model.Coin{Amount: 1000, Denom: "ugnot"},
		PkgPath:        "gno.land/r/demo/foo",
		RefundWithheld: true,
	}
	transferEvent := &model.TransferEvent{
		Type:  "TransferEvent",
		From:  "g1from",
		To:    "g1to",
		Coins: "1000ugnot",
	}

	testTable := []struct {
		event    model.Event
		input    *model.EventInput
		name     string
		expected bool
	}{
		{
			name:     "gno event matches",
			event:    gnoEvent,
			input:    &model.EventInput{GnoEvent: &model.GnoEventInput{Type: ptr("Transfer")}},
			expected: true,
		},
		{
			name:     "gno event input does not match other event types",
			event:    transferEvent,
			input:    &model.EventInput{GnoEvent: &model.GnoEventInput{}},
			expected: false,
		},
		{
			name:  "storage deposit event matches",
			event: depositEvent,
			input: &model.EventInput{StorageDepositEvent: &model.StorageDepositEventInput{
				PkgPath:  ptr("gno.land/r/demo/foo"),
				FeeDelta: &model.CoinInput{Amount: ptr(1000), Denom: ptr("ugnot")},
			}},
			expected: true,
		},
		{
			name:  "storage deposit event fee mismatch",
			event: depositEvent,
			input: &model.EventInput{StorageDepositEvent: &model.StorageDepositEventInput{
				FeeDelta: &model.CoinInput{Amount: ptr(1)},
			}},
			expected: false,
		},
		{
			name:  "storage unlock event matches refund_withheld",
			event: unlockEvent,
			input: &model.EventInput{StorageUnlockEvent: &model.StorageUnlockEventInput{
				RefundWithheld: ptr(true),
				BytesDelta:     ptr(-100),
			}},
			expected: true,
		},
		{
			name:  "storage unlock event refund_withheld mismatch",
			event: unlockEvent,
			input: &model.EventInput{StorageUnlockEvent: &model.StorageUnlockEventInput{
				RefundWithheld: ptr(false),
			}},
			expected: false,
		},
		{
			name:  "transfer event matches",
			event: transferEvent,
			input: &model.EventInput{TransferEvent: &model.TransferEventInput{
				From:  ptr("g1from"),
				To:    ptr("g1to"),
				Coins: &model.AmountInput{From: ptr(500), To: ptr(2000), Denomination: ptr("ugnot")},
			}},
			expected: true,
		},
		{
			name:  "transfer event from mismatch",
			event: transferEvent,
			input: &model.EventInput{TransferEvent: &model.TransferEventInput{
				From: ptr("g1other"),
			}},
			expected: false,
		},
		{
			name:  "transfer event coins out of range",
			event: transferEvent,
			input: &model.EventInput{TransferEvent: &model.TransferEventInput{
				Coins: &model.AmountInput{From: ptr(2000)},
			}},
			expected: false,
		},
		{
			name:     "transfer event input does not match gno event",
			event:    gnoEvent,
			input:    &model.EventInput{TransferEvent: &model.TransferEventInput{}},
			expected: false,
		},
		{
			name:     "unknown event never matches",
			event:    &model.UnknownEvent{Value: "{}"},
			input:    &model.EventInput{GnoEvent: &model.GnoEventInput{}},
			expected: false,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.expected, filteredEventBy(testCase.event, testCase.input))
		})
	}
}
