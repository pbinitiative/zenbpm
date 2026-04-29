//go:build acceptance

package acceptance_test

import (
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	checkProcessId = "KontrolaDodavatelskeFaktury"
	mainProcessId  = "LikvidaceDodavatelskeFaktury"
)

var invoiceApprovalResourcesBPMN = []string{
	"invoice_approval/invoice_check.bpmn",
	"invoice_approval/invoice_liquidation.bpmn",
}
var invoiceApprovalResourcesDMN = []string{
	"invoice_approval/invoice_next_steps.dmn",
	"invoice_approval/invoice_rejection_notification.dmn",
	"invoice_approval/invoice_approvers.dmn",
}

// --- Shared variable-building helpers ---

func whatNextZamitnuto(rejectionCode string) map[string]any {
	return map[string]any{
		"whatNext":                   "Zamitnuto",
		"shouldPayWithoutVat":        false,
		"invoiceRejectionReasonCode": rejectionCode,
	}
}

func whatNextManualne() map[string]any {
	return map[string]any{
		"whatNext":                   "Manualne",
		"shouldPayWithoutVat":        false,
		"invoiceRejectionReasonCode": "",
	}
}

func whatNextPokracovat(shouldPayWithoutVat bool) map[string]any {
	return map[string]any{
		"whatNext":                   "Pokracovat",
		"shouldPayWithoutVat":        shouldPayWithoutVat,
		"invoiceRejectionReasonCode": "",
	}
}

var (
	approvers2 = []any{"TvurceObjednavky", "SpravceRozpoctu"}
	approvers3 = []any{"TvurceObjednavky", "SpravceRozpoctu", "Jednatel"}
)

const (
	descriptionZ1 = "Dobrý den, vracíme Vám fakturu č. <<invoiceNumber>>, neboť k uvedenému plnění neexistuje v naší evidenci odpovídající objednávka. Naše interní procesy neumožňují proplacení dokladů, které nejsou podloženy řádným objednávkovým řízením. Žádáme Vás o prověření situace, případně o doložení dokumentace, která by oprávněnost fakturace prokazovala. S pozdravem, 4BPM Innovations s.r.o."
	descriptionZ2 = "Dobrý den, Vaši fakturu č. <<invoiceNumber>> nemůžeme v současné podobě proplatit. Číslo účtu uvedené na faktuře neodpovídá účtům registrovaným u správce daně. Žádáme Vás o zaslání opravného dokladu s registrovaným bankovním účtem, případně o nápravu v registru plátců DPH. Do té doby je faktura považována za neplatnou. S pozdravem, 4BPM Innovations s.r.o."
	descriptionZ3 = "Dobrý den, vracíme Vám fakturu č. <<invoiceNumber>> k opravě/stornování. Při interním schvalovacím procesu byl u dokladu shledán tento nedostatek: <<invoiceRejectionReason>> Prosíme o opravu dokladu nebo o vyjádření k uvedenému důvodu zamítnutí. Po odstranění tohoto rozporu bude možné fakturu znovu zařadit do platebního procesu. S pozdravem, 4BPM Innovations s.r.o."
)

// --- Scenarios ---

func TestInvoiceApproval_Scenario1_FastRejectNoOrder(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-001"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-001",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-001", 50000)

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"ServiceTask_VytezeniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"ServiceTask_VytezeniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":         "INV-001",
			"invoiceTotalNetAmount": float64(50000),
			"invoiceOrderReference": "ORD-INV-001",
		},
		JobVariables: map[string]map[string]any{
			"ServiceTask_VytezeniFaktury": {"invoiceNumber": "INV-001"},
		},
	})

	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             false,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"SendTask_OznameniZamitnutiFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"SendTask_OznameniZamitnutiFaktury"},
		Variables: map[string]any{
			"invoiceNumber":              "INV-001",
			"invoiceTotalNetAmount":      float64(50000),
			"invoiceOrderReference":      "ORD-INV-001",
			"orderExists":                false,
			"supplierDataMatches":        true,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    true,
			"hasAmountMatch":             true,
			"whatNextResult":             whatNextZamitnuto("Z1"),
			"invoiceRejectionReasonCode": "Z1",
			"title":                      "Zamítnutí faktury č. <<invoiceNumber>> – chybějící objednávka",
			"description":                descriptionZ1,
		},
	})

	completeRejectionNotificationFlow(t, inst.Key)

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":              "INV-001",
			"invoiceTotalNetAmount":      float64(50000),
			"invoiceOrderReference":      "ORD-INV-001",
			"orderExists":                false,
			"supplierDataMatches":        true,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    true,
			"hasAmountMatch":             true,
			"whatNextResult":             whatNextZamitnuto("Z1"),
			"invoiceRejectionReasonCode": "Z1",
			"title":                      "Zamítnutí faktury č. <<invoiceNumber>> – chybějící objednávka",
			"description":                descriptionZ1,
		},
	})

	assertVisited(t, inst.Key,
		"StartEvent_DodavatelskaFakturaProLikvidaci",
		"UserTask_ManualniNahraniFaktury",
		"ServiceTask_VytezeniFaktury",
		"UserTask_ManualniKorekcePoVytezeni",
		"CallActivity_KontrolaFaktury",
		"BusinessRuleTask_StanoveniDalsihoPostupu",
		"Gateway_CoDal",
		"Flow_Zamitnout",
		"BusinessRuleTask_StanoveniObsahuOznameni",
		"SendTask_OznameniZamitnutiFaktury",
		"EndEvent_FakturaZamitnuta",
	)
	assertNotVisited(t, inst.Key,
		"Flow_ManualneOverit",
		"Flow_PokracovatVeZpracovani",
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"UserTask_SchvaleniFaktury",
		"UserTask_ManualniOvereniFaktury",
	)
}

func TestInvoiceApproval_Scenario2_FastRejectBankAccountNotRegistered(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-002"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-002",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-002", 30000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": false,
		"hasAmountMatch":          true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"SendTask_OznameniZamitnutiFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"SendTask_OznameniZamitnutiFaktury"},
		Variables: map[string]any{
			"invoiceNumber":              "INV-002",
			"invoiceTotalNetAmount":      float64(30000),
			"invoiceOrderReference":      "ORD-INV-002",
			"orderExists":                true,
			"supplierDataMatches":        true,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    false,
			"hasAmountMatch":             true,
			"whatNextResult":             whatNextZamitnuto("Z2"),
			"invoiceRejectionReasonCode": "Z2",
			"title":                      "Zamítnutí faktury č. <<invoiceNumber>> – neregistrovaný bankovní účet",
			"description":                descriptionZ2,
		},
	})

	completeRejectionNotificationFlow(t, inst.Key)

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":              "INV-002",
			"invoiceTotalNetAmount":      float64(30000),
			"invoiceOrderReference":      "ORD-INV-002",
			"orderExists":                true,
			"supplierDataMatches":        true,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    false,
			"hasAmountMatch":             true,
			"whatNextResult":             whatNextZamitnuto("Z2"),
			"invoiceRejectionReasonCode": "Z2",
			"title":                      "Zamítnutí faktury č. <<invoiceNumber>> – neregistrovaný bankovní účet",
			"description":                descriptionZ2,
		},
	})

	assertVisited(t, inst.Key,
		"BusinessRuleTask_StanoveniDalsihoPostupu",
		"Gateway_CoDal",
		"BusinessRuleTask_StanoveniObsahuOznameni",
		"SendTask_OznameniZamitnutiFaktury",
		"EndEvent_FakturaZamitnuta",
	)
	assertNotVisited(t, inst.Key,
		"UserTask_ManualniOvereniFaktury",
		"BusinessRuleTask_StanoveniSchvalovatelu",
	)
}

func TestInvoiceApproval_Scenario3_ManualReviewThenReject(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-003"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-003",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-003", 20000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     false,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniOvereniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniOvereniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":              "INV-003",
			"invoiceTotalNetAmount":      float64(20000),
			"invoiceOrderReference":      "ORD-INV-003",
			"orderExists":                true,
			"supplierDataMatches":        false,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    true,
			"hasAmountMatch":             true,
			"whatNextResult":             whatNextManualne(),
			"invoiceRejectionReasonCode": "",
		},
	})

	reviewJob := waitForJob(t, inst.Key, "UserTask_ManualniOvereniFaktury")
	completeJob(t, reviewJob.Key, map[string]any{
		"poslatKeSchvaleni":          false,
		"invoiceRejectionReasonCode": "Z3",
	})

	completeRejectionNotificationFlow(t, inst.Key)

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":              "INV-003",
			"invoiceTotalNetAmount":      float64(20000),
			"invoiceOrderReference":      "ORD-INV-003",
			"orderExists":                true,
			"supplierDataMatches":        false,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    true,
			"hasAmountMatch":             true,
			"whatNextResult":             whatNextManualne(),
			"invoiceRejectionReasonCode": "Z3",
			"poslatKeSchvaleni":          false,
			"title":                      "Neuznání faktury č. <<invoiceNumber>>",
			"description":                descriptionZ3,
		},
	})

	assertVisited(t, inst.Key,
		"Gateway_CoDal",
		"UserTask_ManualniOvereniFaktury",
		"Gateway_1rk8z7u",
		"BusinessRuleTask_StanoveniObsahuOznameni",
		"SendTask_OznameniZamitnutiFaktury",
		"EndEvent_FakturaZamitnuta",
	)
	assertNotVisited(t, inst.Key,
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"UserTask_SchvaleniFaktury",
	)
}

func TestInvoiceApproval_Scenario4_ManualReviewThenApprovePayByTransfer(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-004"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-004",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-004", 80000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     false,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniOvereniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniOvereniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":              "INV-004",
			"invoiceTotalNetAmount":      float64(80000),
			"invoiceOrderReference":      "ORD-INV-004",
			"orderExists":                true,
			"supplierDataMatches":        false,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    true,
			"hasAmountMatch":             true,
			"whatNextResult":             whatNextManualne(),
			"invoiceRejectionReasonCode": "",
		},
	})

	reviewJob := waitForJob(t, inst.Key, "UserTask_ManualniOvereniFaktury")
	completeJob(t, reviewJob.Key, map[string]any{
		"poslatKeSchvaleni": true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_SchvaleniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_SchvaleniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":                "INV-004",
			"invoiceTotalNetAmount":        float64(80000),
			"invoiceOrderReference":        "ORD-INV-004",
			"orderExists":                  true,
			"supplierDataMatches":          false,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextManualne(),
			"invoiceRejectionReasonCode":   "",
			"poslatKeSchvaleni":            true,
			"designationOfApproversResult": approvers2,
		},
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{"schvaleno": true})

	completeApprovalFlow(t, inst.Key, true, map[string]any{
		"invoiceNumber":                "INV-004",
		"invoiceTotalNetAmount":        float64(80000),
		"invoiceOrderReference":        "ORD-INV-004",
		"orderExists":                  true,
		"supplierDataMatches":          false,
		"isReliableVatPayer":           true,
		"isBankAccountRegistered":      true,
		"hasAmountMatch":               true,
		"whatNextResult":               whatNextManualne(),
		"invoiceRejectionReasonCode":   "",
		"poslatKeSchvaleni":            true,
		"designationOfApproversResult": approvers2,
		"schvaleno":                    true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":                "INV-004",
			"invoiceTotalNetAmount":        float64(80000),
			"invoiceOrderReference":        "ORD-INV-004",
			"orderExists":                  true,
			"supplierDataMatches":          false,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextManualne(),
			"invoiceRejectionReasonCode":   "",
			"poslatKeSchvaleni":            true,
			"designationOfApproversResult": approvers2,
			"schvaleno":                    true,
			"payPrevodem":                  true,
			"payZapoctem":                  false,
		},
	})

	assertVisited(t, inst.Key,
		"UserTask_ManualniOvereniFaktury",
		"Gateway_1rk8z7u",
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"UserTask_SchvaleniFaktury",
		"Gateway_16m8kpr",
		"SendTask_OznameniSchvaleniFaktury",
		"UserTask_KonfiguraceUctovaniFaktury",
		"ServiceTask_ZauctovaniDodavatelskeFaktury",
		"Gateway_FormaUhrady",
		"ServiceTask_VytvoreniNavrhuPrikazuPlatby",
		"EndEvent_DodavatelskaFakturaZlikvidovanaAProplacena",
	)
	assertNotVisited(t, inst.Key,
		"EndEvent_FakturaZamitnuta",
		"UserTask_ZapocetFaktury",
	)
}

func TestInvoiceApproval_Scenario5_AllChecksPassApprovePayByOffset(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-005"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-005",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-005", 50000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_SchvaleniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_SchvaleniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":                "INV-005",
			"invoiceTotalNetAmount":        float64(50000),
			"invoiceOrderReference":        "ORD-INV-005",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(false),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers2,
		},
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{"schvaleno": true})

	s5Base := map[string]any{
		"invoiceNumber":                "INV-005",
		"invoiceTotalNetAmount":        float64(50000),
		"invoiceOrderReference":        "ORD-INV-005",
		"orderExists":                  true,
		"supplierDataMatches":          true,
		"isReliableVatPayer":           true,
		"isBankAccountRegistered":      true,
		"hasAmountMatch":               true,
		"whatNextResult":               whatNextPokracovat(false),
		"invoiceRejectionReasonCode":   "",
		"designationOfApproversResult": approvers2,
		"schvaleno":                    true,
	}

	approvalSendJob := waitForJob(t, inst.Key, "SendTask_OznameniSchvaleniFaktury")
	completeJob(t, approvalSendJob.Key, map[string]any{})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_KonfiguraceUctovaniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_KonfiguraceUctovaniFaktury"},
		Variables:    s5Base,
	})

	configJob := waitForJob(t, inst.Key, "UserTask_KonfiguraceUctovaniFaktury")
	completeJob(t, configJob.Key, map[string]any{"payPrevodem": false, "payZapoctem": true})

	s5Config := maps.Clone(s5Base)
	s5Config["payPrevodem"] = false
	s5Config["payZapoctem"] = true

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"ServiceTask_ZauctovaniDodavatelskeFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"ServiceTask_ZauctovaniDodavatelskeFaktury"},
		Variables:    s5Config,
	})

	accountJob := waitForJob(t, inst.Key, "ServiceTask_ZauctovaniDodavatelskeFaktury")
	completeJob(t, accountJob.Key, map[string]any{})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ZapocetFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ZapocetFaktury"},
		Variables:    s5Config,
	})

	offsetJob := waitForJob(t, inst.Key, "UserTask_ZapocetFaktury")
	completeJob(t, offsetJob.Key, map[string]any{})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:     zenclient.ProcessInstanceStateCompleted,
		Variables: s5Config,
	})

	assertVisited(t, inst.Key,
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"UserTask_SchvaleniFaktury",
		"SendTask_OznameniSchvaleniFaktury",
		"Gateway_FormaUhrady",
		"UserTask_ZapocetFaktury",
		"EndEvent_DodavatelskaFakturaZlikvidovanaZapoctem",
	)
	assertNotVisited(t, inst.Key,
		"UserTask_ManualniOvereniFaktury",
		"ServiceTask_VytvoreniNavrhuPrikazuPlatby",
		"EndEvent_FakturaZamitnuta",
	)
}

func TestInvoiceApproval_Scenario6_HighValueInvoiceThreeApprovers(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-006"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-006",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-006", 150000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assert.Eventually(t, func() bool {
		result, ok := getProcessInstance(t, inst.Key).Variables["designationOfApproversResult"]
		if !ok {
			return false
		}
		list, ok := result.([]any)
		return ok && len(list) == 3
	}, 10*time.Second, 200*time.Millisecond, "expected 3 approvers for high-value invoice")

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_SchvaleniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_SchvaleniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":                "INV-006",
			"invoiceTotalNetAmount":        float64(150000),
			"invoiceOrderReference":        "ORD-INV-006",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(false),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers3,
		},
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{"schvaleno": true})

	completeApprovalFlow(t, inst.Key, true, map[string]any{
		"invoiceNumber":                "INV-006",
		"invoiceTotalNetAmount":        float64(150000),
		"invoiceOrderReference":        "ORD-INV-006",
		"orderExists":                  true,
		"supplierDataMatches":          true,
		"isReliableVatPayer":           true,
		"isBankAccountRegistered":      true,
		"hasAmountMatch":               true,
		"whatNextResult":               whatNextPokracovat(false),
		"invoiceRejectionReasonCode":   "",
		"designationOfApproversResult": approvers3,
		"schvaleno":                    true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":                "INV-006",
			"invoiceTotalNetAmount":        float64(150000),
			"invoiceOrderReference":        "ORD-INV-006",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(false),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers3,
			"schvaleno":                    true,
			"payPrevodem":                  true,
			"payZapoctem":                  false,
		},
	})

	assertVisited(t, inst.Key,
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"EndEvent_DodavatelskaFakturaZlikvidovanaAProplacena",
	)
}

func TestInvoiceApproval_Scenario7_ApprovalRejectedByApprover(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-007"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-007",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-007", 60000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_SchvaleniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_SchvaleniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":                "INV-007",
			"invoiceTotalNetAmount":        float64(60000),
			"invoiceOrderReference":        "ORD-INV-007",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(false),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers2,
		},
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{
		"schvaleno":                  false,
		"invoiceRejectionReasonCode": "Z3",
	})

	completeRejectionNotificationFlow(t, inst.Key)

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":                "INV-007",
			"invoiceTotalNetAmount":        float64(60000),
			"invoiceOrderReference":        "ORD-INV-007",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(false),
			"invoiceRejectionReasonCode":   "Z3",
			"designationOfApproversResult": approvers2,
			"schvaleno":                    false,
			"title":                        "Neuznání faktury č. <<invoiceNumber>>",
			"description":                  descriptionZ3,
		},
	})

	assertVisited(t, inst.Key,
		"UserTask_SchvaleniFaktury",
		"Gateway_16m8kpr",
		"BusinessRuleTask_StanoveniObsahuOznameni",
		"SendTask_OznameniZamitnutiFaktury",
		"EndEvent_FakturaZamitnuta",
	)
	assertNotVisited(t, inst.Key,
		"SendTask_OznameniSchvaleniFaktury",
		"EndEvent_DodavatelskaFakturaZlikvidovanaAProplacena",
		"EndEvent_DodavatelskaFakturaZlikvidovanaZapoctem",
	)
}

func TestInvoiceApproval_Scenario8_AmountMismatchManualReviewThenReject(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-008"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-008",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-008", 40000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          false,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniOvereniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniOvereniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":              "INV-008",
			"invoiceTotalNetAmount":      float64(40000),
			"invoiceOrderReference":      "ORD-INV-008",
			"orderExists":                true,
			"supplierDataMatches":        true,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    true,
			"hasAmountMatch":             false,
			"whatNextResult":             whatNextManualne(),
			"invoiceRejectionReasonCode": "",
		},
	})

	reviewJob := waitForJob(t, inst.Key, "UserTask_ManualniOvereniFaktury")
	completeJob(t, reviewJob.Key, map[string]any{
		"poslatKeSchvaleni":          false,
		"invoiceRejectionReasonCode": "Z3",
	})

	completeRejectionNotificationFlow(t, inst.Key)

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":              "INV-008",
			"invoiceTotalNetAmount":      float64(40000),
			"invoiceOrderReference":      "ORD-INV-008",
			"orderExists":                true,
			"supplierDataMatches":        true,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    true,
			"hasAmountMatch":             false,
			"whatNextResult":             whatNextManualne(),
			"invoiceRejectionReasonCode": "Z3",
			"poslatKeSchvaleni":          false,
			"title":                      "Neuznání faktury č. <<invoiceNumber>>",
			"description":                descriptionZ3,
		},
	})

	assertVisited(t, inst.Key,
		"Flow_ManualneOverit",
		"UserTask_ManualniOvereniFaktury",
		"Gateway_1rk8z7u",
		"BusinessRuleTask_StanoveniObsahuOznameni",
		"SendTask_OznameniZamitnutiFaktury",
		"EndEvent_FakturaZamitnuta",
	)
	assertNotVisited(t, inst.Key,
		"Flow_Zamitnout",
		"Flow_PokracovatVeZpracovani",
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"UserTask_SchvaleniFaktury",
	)
}

func TestInvoiceApproval_Scenario9_ManualReviewApprovedThenApproverRejects(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-009"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-009",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-009", 50000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     false,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniOvereniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniOvereniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":              "INV-009",
			"invoiceTotalNetAmount":      float64(50000),
			"invoiceOrderReference":      "ORD-INV-009",
			"orderExists":                true,
			"supplierDataMatches":        false,
			"isReliableVatPayer":         true,
			"isBankAccountRegistered":    true,
			"hasAmountMatch":             true,
			"whatNextResult":             whatNextManualne(),
			"invoiceRejectionReasonCode": "",
		},
	})

	reviewJob := waitForJob(t, inst.Key, "UserTask_ManualniOvereniFaktury")
	completeJob(t, reviewJob.Key, map[string]any{
		"poslatKeSchvaleni": true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_SchvaleniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_SchvaleniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":                "INV-009",
			"invoiceTotalNetAmount":        float64(50000),
			"invoiceOrderReference":        "ORD-INV-009",
			"orderExists":                  true,
			"supplierDataMatches":          false,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextManualne(),
			"invoiceRejectionReasonCode":   "",
			"poslatKeSchvaleni":            true,
			"designationOfApproversResult": approvers2,
		},
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{
		"schvaleno":                  false,
		"invoiceRejectionReasonCode": "Z3",
	})

	completeRejectionNotificationFlow(t, inst.Key)

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":                "INV-009",
			"invoiceTotalNetAmount":        float64(50000),
			"invoiceOrderReference":        "ORD-INV-009",
			"orderExists":                  true,
			"supplierDataMatches":          false,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextManualne(),
			"invoiceRejectionReasonCode":   "Z3",
			"poslatKeSchvaleni":            true,
			"designationOfApproversResult": approvers2,
			"schvaleno":                    false,
			"title":                        "Neuznání faktury č. <<invoiceNumber>>",
			"description":                  descriptionZ3,
		},
	})

	assertVisited(t, inst.Key,
		"Flow_ManualneOverit",
		"UserTask_ManualniOvereniFaktury",
		"Gateway_1rk8z7u",
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"UserTask_SchvaleniFaktury",
		"Gateway_16m8kpr",
		"BusinessRuleTask_StanoveniObsahuOznameni",
		"SendTask_OznameniZamitnutiFaktury",
		"EndEvent_FakturaZamitnuta",
	)
	assertNotVisited(t, inst.Key,
		"SendTask_OznameniSchvaleniFaktury",
		"EndEvent_DodavatelskaFakturaZlikvidovanaAProplacena",
		"EndEvent_DodavatelskaFakturaZlikvidovanaZapoctem",
	)
}

func TestInvoiceApproval_Scenario10_UnreliableVatPayerBranchAndShouldPayWithoutVat(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-010"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-010",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-010", 45000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	childKey := completeInvoiceCheckSubProcessReturnChildKey(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      false,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assertVisited(t, childKey, "ServiceTask_AktualizaceDatDodavatele")

	assert.Eventually(t, func() bool {
		vars := getProcessInstance(t, inst.Key).Variables
		whatNextResult, ok := vars["whatNextResult"]
		if !ok {
			return false
		}
		m, ok := whatNextResult.(map[string]any)
		if !ok {
			return false
		}
		return m["shouldPayWithoutVat"] == true
	}, 10*time.Second, 200*time.Millisecond,
		"expected shouldPayWithoutVat=true in whatNextResult for unreliable VAT payer")

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_SchvaleniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_SchvaleniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":                "INV-010",
			"invoiceTotalNetAmount":        float64(45000),
			"invoiceOrderReference":        "ORD-INV-010",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           false,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(true),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers2,
		},
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{"schvaleno": true})

	completeApprovalFlow(t, inst.Key, true, map[string]any{
		"invoiceNumber":                "INV-010",
		"invoiceTotalNetAmount":        float64(45000),
		"invoiceOrderReference":        "ORD-INV-010",
		"orderExists":                  true,
		"supplierDataMatches":          true,
		"isReliableVatPayer":           false,
		"isBankAccountRegistered":      true,
		"hasAmountMatch":               true,
		"whatNextResult":               whatNextPokracovat(true),
		"invoiceRejectionReasonCode":   "",
		"designationOfApproversResult": approvers2,
		"schvaleno":                    true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":                "INV-010",
			"invoiceTotalNetAmount":        float64(45000),
			"invoiceOrderReference":        "ORD-INV-010",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           false,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(true),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers2,
			"schvaleno":                    true,
			"payPrevodem":                  true,
			"payZapoctem":                  false,
		},
	})

	assertVisited(t, inst.Key,
		"Flow_PokracovatVeZpracovani",
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"UserTask_SchvaleniFaktury",
		"SendTask_OznameniSchvaleniFaktury",
		"ServiceTask_ZauctovaniDodavatelskeFaktury",
		"ServiceTask_VytvoreniNavrhuPrikazuPlatby",
		"EndEvent_DodavatelskaFakturaZlikvidovanaAProplacena",
	)
	assertNotVisited(t, inst.Key,
		"Flow_Zamitnout",
		"Flow_ManualneOverit",
		"EndEvent_FakturaZamitnuta",
	)
}

func TestInvoiceApproval_Scenario11_PayByBothTransferAndOffset(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-011"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ManualniNahraniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ManualniNahraniFaktury"},
		Variables: map[string]any{
			"invoiceNumber": "INV-011",
		},
	})

	completeUploadUserTask(t, inst.Key, "INV-011", 55000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_SchvaleniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_SchvaleniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":                "INV-011",
			"invoiceTotalNetAmount":        float64(55000),
			"invoiceOrderReference":        "ORD-INV-011",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(false),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers2,
		},
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{"schvaleno": true})

	s11Base := map[string]any{
		"invoiceNumber":                "INV-011",
		"invoiceTotalNetAmount":        float64(55000),
		"invoiceOrderReference":        "ORD-INV-011",
		"orderExists":                  true,
		"supplierDataMatches":          true,
		"isReliableVatPayer":           true,
		"isBankAccountRegistered":      true,
		"hasAmountMatch":               true,
		"whatNextResult":               whatNextPokracovat(false),
		"invoiceRejectionReasonCode":   "",
		"designationOfApproversResult": approvers2,
		"schvaleno":                    true,
	}

	approvalSendJob := waitForJob(t, inst.Key, "SendTask_OznameniSchvaleniFaktury")
	completeJob(t, approvalSendJob.Key, map[string]any{})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_KonfiguraceUctovaniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_KonfiguraceUctovaniFaktury"},
		Variables:    s11Base,
	})

	configJob := waitForJob(t, inst.Key, "UserTask_KonfiguraceUctovaniFaktury")
	completeJob(t, configJob.Key, map[string]any{
		"payPrevodem": true,
		"payZapoctem": true,
	})

	s11Config := maps.Clone(s11Base)
	s11Config["payPrevodem"] = true
	s11Config["payZapoctem"] = true

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"ServiceTask_ZauctovaniDodavatelskeFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"ServiceTask_ZauctovaniDodavatelskeFaktury"},
		Variables:    s11Config,
	})

	accountJob := waitForJob(t, inst.Key, "ServiceTask_ZauctovaniDodavatelskeFaktury")
	completeJob(t, accountJob.Key, map[string]any{})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{
			{"ServiceTask_VytvoreniNavrhuPrikazuPlatby", "TokenStateWaiting"},
			{"UserTask_ZapocetFaktury", "TokenStateWaiting"},
		},
		ActiveJobs: []string{
			"ServiceTask_VytvoreniNavrhuPrikazuPlatby",
			"UserTask_ZapocetFaktury",
		},
		Variables: s11Config,
	})

	transferJob := waitForJob(t, inst.Key, "ServiceTask_VytvoreniNavrhuPrikazuPlatby")
	completeJob(t, transferJob.Key, map[string]any{})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_ZapocetFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_ZapocetFaktury"},
		Variables:    s11Config,
	})

	offsetJob := waitForJob(t, inst.Key, "UserTask_ZapocetFaktury")
	completeJob(t, offsetJob.Key, map[string]any{})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:     zenclient.ProcessInstanceStateCompleted,
		Variables: s11Config,
	})

	assertVisited(t, inst.Key,
		"Gateway_FormaUhrady",
		"ServiceTask_VytvoreniNavrhuPrikazuPlatby",
		"UserTask_ZapocetFaktury",
		"EndEvent_DodavatelskaFakturaZlikvidovanaAProplacena",
		"EndEvent_DodavatelskaFakturaZlikvidovanaZapoctem",
	)
}

func TestInvoiceApproval_Scenario12_NoPaymentMethodSelectedCausesIncident(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-012"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	completeUploadUserTask(t, inst.Key, "INV-012", 50000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{"schvaleno": true})

	s12Base := map[string]any{
		"invoiceNumber":                "INV-012",
		"invoiceTotalNetAmount":        float64(50000),
		"invoiceOrderReference":        "ORD-INV-012",
		"orderExists":                  true,
		"supplierDataMatches":          true,
		"isReliableVatPayer":           true,
		"isBankAccountRegistered":      true,
		"hasAmountMatch":               true,
		"whatNextResult":               whatNextPokracovat(false),
		"invoiceRejectionReasonCode":   "",
		"designationOfApproversResult": approvers2,
		"schvaleno":                    true,
	}

	approvalSendJob := waitForJob(t, inst.Key, "SendTask_OznameniSchvaleniFaktury")
	completeJob(t, approvalSendJob.Key, map[string]any{})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_KonfiguraceUctovaniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_KonfiguraceUctovaniFaktury"},
		Variables:    s12Base,
	})

	configJob := waitForJob(t, inst.Key, "UserTask_KonfiguraceUctovaniFaktury")
	completeJob(t, configJob.Key, map[string]any{
		"payPrevodem": false,
		"payZapoctem": false,
	})

	s12Config := maps.Clone(s12Base)
	s12Config["payPrevodem"] = false
	s12Config["payZapoctem"] = false

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"ServiceTask_ZauctovaniDodavatelskeFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"ServiceTask_ZauctovaniDodavatelskeFaktury"},
		Variables:    s12Config,
	})

	accountJob := waitForJob(t, inst.Key, "ServiceTask_ZauctovaniDodavatelskeFaktury")
	vars := map[string]any{}
	completeJobResp, err := app.restClient.CompleteJobWithResponse(t.Context(), accountJob.Key,
		zenclient.CompleteJobJSONRequestBody{Variables: &vars})
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, completeJobResp.StatusCode(),
		"expected HTTP 500: engine error from gateway with no valid outgoing flow")

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateFailed,
		Incidents:    true,
		ActiveTokens: []tokenExpectation{{"Gateway_FormaUhrady", "TokenStateFailed"}},
		Variables:    s12Config,
	})
	assertNotVisited(t, inst.Key,
		"EndEvent_DodavatelskaFakturaZlikvidovanaAProplacena",
		"EndEvent_DodavatelskaFakturaZlikvidovanaZapoctem",
		"EndEvent_FakturaZamitnuta",
	)
}

func TestInvoiceApproval_Scenario13_ExactBoundaryAmountTwoApprovers(t *testing.T) {
	deployInvoiceApprovalResources(t)

	inst := startProcess(t, mainProcessId, map[string]any{"invoiceNumber": "INV-013"})
	t.Cleanup(func() { cancelProcess(t, inst.Key) })

	completeUploadUserTask(t, inst.Key, "INV-013", 100000)
	completeOcrJob(t, inst.Key)
	completeManualCorrectionTask(t, inst.Key)

	completeInvoiceCheckSubProcess(t, inst.Key, map[string]any{
		"orderExists":             true,
		"supplierDataMatches":     true,
		"isReliableVatPayer":      true,
		"isBankAccountRegistered": true,
		"hasAmountMatch":          true,
	})

	assert.Eventually(t, func() bool {
		result, ok := getProcessInstance(t, inst.Key).Variables["designationOfApproversResult"]
		if !ok {
			return false
		}
		list, ok := result.([]any)
		return ok && len(list) == 2
	}, 10*time.Second, 200*time.Millisecond,
		"expected exactly 2 approvers for invoiceTotalNetAmount=100000")

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_SchvaleniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_SchvaleniFaktury"},
		Variables: map[string]any{
			"invoiceNumber":                "INV-013",
			"invoiceTotalNetAmount":        float64(100000),
			"invoiceOrderReference":        "ORD-INV-013",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(false),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers2,
		},
	})

	approvalJob := waitForJob(t, inst.Key, "UserTask_SchvaleniFaktury")
	completeJob(t, approvalJob.Key, map[string]any{"schvaleno": true})

	completeApprovalFlow(t, inst.Key, true, map[string]any{
		"invoiceNumber":                "INV-013",
		"invoiceTotalNetAmount":        float64(100000),
		"invoiceOrderReference":        "ORD-INV-013",
		"orderExists":                  true,
		"supplierDataMatches":          true,
		"isReliableVatPayer":           true,
		"isBankAccountRegistered":      true,
		"hasAmountMatch":               true,
		"whatNextResult":               whatNextPokracovat(false),
		"invoiceRejectionReasonCode":   "",
		"designationOfApproversResult": approvers2,
		"schvaleno":                    true,
	})

	assertProcessCheckpoint(t, inst.Key, ProcessCheckpoint{
		State: zenclient.ProcessInstanceStateCompleted,
		Variables: map[string]any{
			"invoiceNumber":                "INV-013",
			"invoiceTotalNetAmount":        float64(100000),
			"invoiceOrderReference":        "ORD-INV-013",
			"orderExists":                  true,
			"supplierDataMatches":          true,
			"isReliableVatPayer":           true,
			"isBankAccountRegistered":      true,
			"hasAmountMatch":               true,
			"whatNextResult":               whatNextPokracovat(false),
			"invoiceRejectionReasonCode":   "",
			"designationOfApproversResult": approvers2,
			"schvaleno":                    true,
			"payPrevodem":                  true,
			"payZapoctem":                  false,
		},
	})

	assertVisited(t, inst.Key,
		"BusinessRuleTask_StanoveniSchvalovatelu",
		"UserTask_SchvaleniFaktury",
		"EndEvent_DodavatelskaFakturaZlikvidovanaAProplacena",
	)
	assertNotVisited(t, inst.Key,
		"EndEvent_FakturaZamitnuta",
		"UserTask_ManualniOvereniFaktury",
	)
}

// --- Scenario helpers ---

func deployInvoiceApprovalResources(t testing.TB) {
	t.Helper()
	for _, res := range invoiceApprovalResourcesBPMN {
		deployBpmn(t, res)
	}
	for _, res := range invoiceApprovalResourcesDMN {
		deployDmn(t, res)
	}
}

func completeOcrJob(t testing.TB, mainKey int64) {
	t.Helper()
	job := waitForJob(t, mainKey, "ServiceTask_VytezeniFaktury")
	completeJob(t, job.Key, map[string]any{})
}

func completeUploadUserTask(t testing.TB, mainKey int64, invoiceNumber string, totalNetAmount float64) {
	t.Helper()
	job := waitForJob(t, mainKey, "UserTask_ManualniNahraniFaktury")
	completeJob(t, job.Key, map[string]any{
		"invoiceNumber":         invoiceNumber,
		"invoiceTotalNetAmount": totalNetAmount,
		"invoiceOrderReference": "ORD-" + invoiceNumber,
	})
}

func completeManualCorrectionTask(t testing.TB, mainKey int64) {
	t.Helper()
	job := waitForJob(t, mainKey, "UserTask_ManualniKorekcePoVytezeni")
	completeJob(t, job.Key, map[string]any{})
}

func completeInvoiceCheckSubProcess(t testing.TB, mainKey int64, outputs map[string]any) {
	t.Helper()
	completeInvoiceCheckSubProcessReturnChildKey(t, mainKey, outputs)
}

func completeInvoiceCheckSubProcessReturnChildKey(t testing.TB, mainKey int64, outputs map[string]any) int64 {
	t.Helper()

	var childKey int64
	var childVars map[string]any
	assert.Eventually(t, func() bool {
		children := getChildInstances(t, mainKey)
		if len(children) == 0 {
			return false
		}
		childKey = children[0].Key
		vars := getProcessInstance(t, childKey).Variables
		if _, ok := vars["invoiceOrderReference"]; !ok {
			return false
		}
		childVars = maps.Clone(vars)
		return true
	}, 10*time.Second, 200*time.Millisecond,
		"child invoice-check process not found / invoiceOrderReference not set for parent %d", mainKey)

	const (
		taskExistence    = "ServiceTask_OvereniExistenceObjednavky"
		taskVatPayer     = "ServiceTask_KontrolaSpolehlivostiPlatceDPH"
		taskUpdateVendor = "ServiceTask_AktualizaceDatDodavatele"
		taskBankAccount  = "ServiceTask_KontrolaRegistraceBankovnihoUctu"
		taskVendorData   = "ServiceTask_OvereniUdajuDodavatele"
		taskMathCheck    = "ServiceTask_MatematickaKontrolaFaktury"
	)

	remaining := []string{taskExistence, taskVatPayer, taskBankAccount, taskVendorData, taskMathCheck}

	assertProcessCheckpoint(t, childKey, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: toWaitingTokenExpectations(remaining),
		ActiveJobs:   append([]string{}, remaining...),
		Variables:    maps.Clone(childVars),
	})

	type jobStep struct {
		elementID string
		outputKey string
	}
	steps := []jobStep{
		{taskExistence, "orderExists"},
		{taskVatPayer, "isReliableVatPayer"},
		{taskBankAccount, "isBankAccountRegistered"},
		{taskVendorData, "supplierDataMatches"},
		{taskMathCheck, "hasAmountMatch"},
	}

	for _, step := range steps {
		j := waitForJob(t, childKey, step.elementID)
		val, ok := outputs[step.outputKey]
		if !ok {
			val = true
		}
		completeJob(t, j.Key, map[string]any{step.outputKey: val})
		childVars[step.outputKey] = val

		remaining = removeString(remaining, step.elementID)

		if step.elementID == taskVatPayer {
			if reliable, _ := val.(bool); !reliable {
				withUpdate := append([]string{taskUpdateVendor}, remaining...)
				assertProcessCheckpoint(t, childKey, ProcessCheckpoint{
					State:        zenclient.ProcessInstanceStateActive,
					ActiveTokens: toWaitingTokenExpectations(withUpdate),
					ActiveJobs:   append([]string{}, withUpdate...),
					Variables:    maps.Clone(childVars),
				})

				updateJob := waitForJob(t, childKey, taskUpdateVendor)
				completeJob(t, updateJob.Key, map[string]any{"isReliableVatPayer": false})
			}
		}

		if len(remaining) > 0 {
			assertProcessCheckpoint(t, childKey, ProcessCheckpoint{
				State:        zenclient.ProcessInstanceStateActive,
				ActiveTokens: toWaitingTokenExpectations(remaining),
				ActiveJobs:   append([]string{}, remaining...),
				Variables:    maps.Clone(childVars),
			})
		}
	}

	assertProcessCheckpoint(t, childKey, ProcessCheckpoint{
		State:     zenclient.ProcessInstanceStateCompleted,
		Variables: maps.Clone(childVars),
	})

	return childKey
}

func completeRejectionNotificationFlow(t testing.TB, mainKey int64) {
	t.Helper()
	sendJob := waitForJob(t, mainKey, "SendTask_OznameniZamitnutiFaktury")
	completeJob(t, sendJob.Key, map[string]any{})
}

func completeApprovalFlow(t testing.TB, mainKey int64, payByTransfer bool, baseVars map[string]any) {
	t.Helper()

	approvalSendJob := waitForJob(t, mainKey, "SendTask_OznameniSchvaleniFaktury")
	completeJob(t, approvalSendJob.Key, map[string]any{})

	assertProcessCheckpoint(t, mainKey, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"UserTask_KonfiguraceUctovaniFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"UserTask_KonfiguraceUctovaniFaktury"},
		Variables:    baseVars,
	})

	configJob := waitForJob(t, mainKey, "UserTask_KonfiguraceUctovaniFaktury")
	completeJob(t, configJob.Key, map[string]any{
		"payPrevodem": payByTransfer,
		"payZapoctem": !payByTransfer,
	})

	configVars := maps.Clone(baseVars)
	configVars["payPrevodem"] = payByTransfer
	configVars["payZapoctem"] = !payByTransfer

	assertProcessCheckpoint(t, mainKey, ProcessCheckpoint{
		State:        zenclient.ProcessInstanceStateActive,
		ActiveTokens: []tokenExpectation{{"ServiceTask_ZauctovaniDodavatelskeFaktury", "TokenStateWaiting"}},
		ActiveJobs:   []string{"ServiceTask_ZauctovaniDodavatelskeFaktury"},
		Variables:    configVars,
	})

	accountJob := waitForJob(t, mainKey, "ServiceTask_ZauctovaniDodavatelskeFaktury")
	completeJob(t, accountJob.Key, map[string]any{})

	if payByTransfer {
		assertProcessCheckpoint(t, mainKey, ProcessCheckpoint{
			State:        zenclient.ProcessInstanceStateActive,
			ActiveTokens: []tokenExpectation{{"ServiceTask_VytvoreniNavrhuPrikazuPlatby", "TokenStateWaiting"}},
			ActiveJobs:   []string{"ServiceTask_VytvoreniNavrhuPrikazuPlatby"},
			Variables:    configVars,
		})
		paymentJob := waitForJob(t, mainKey, "ServiceTask_VytvoreniNavrhuPrikazuPlatby")
		completeJob(t, paymentJob.Key, map[string]any{})
	} else {
		assertProcessCheckpoint(t, mainKey, ProcessCheckpoint{
			State:        zenclient.ProcessInstanceStateActive,
			ActiveTokens: []tokenExpectation{{"UserTask_ZapocetFaktury", "TokenStateWaiting"}},
			ActiveJobs:   []string{"UserTask_ZapocetFaktury"},
			Variables:    configVars,
		})
		offsetJob := waitForJob(t, mainKey, "UserTask_ZapocetFaktury")
		completeJob(t, offsetJob.Key, map[string]any{})
	}
}

// --- Utilities used by the subprocess helper ---

func toWaitingTokenExpectations(elementIDs []string) []tokenExpectation {
	out := make([]tokenExpectation, len(elementIDs))
	for i, id := range elementIDs {
		out[i] = tokenExpectation{elementID: id, state: "TokenStateWaiting"}
	}
	return out
}

func removeString(slice []string, s string) []string {
	result := make([]string, 0, len(slice))
	removed := false
	for _, v := range slice {
		if !removed && v == s {
			removed = true
			continue
		}
		result = append(result, v)
	}
	return result
}
