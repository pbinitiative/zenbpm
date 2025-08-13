package dmn

type TDefinitions struct {
	Id              string      `xml:"id,attr"`
	Name            string      `xml:"name,attr"`
	Namespace       string      `xml:"namespace,attr"`
	Exporter        string      `xml:"exporter,attr"`
	ExporterVersion string      `xml:"exporterVersion,attr"`
	Decisions       []TDecision `xml:"decision"`
	DMNDI           TDMNDI      `xml:"dmndi:DMNDI"`
}

type TDecision struct {
	Id                     string                    `xml:"id,attr"`
	Name                   string                    `xml:"name,attr"`
	DecisionTable          TDecisionTable            `xml:"decisionTable"`
	Variable               TVariable                 `xml:"variable"`
	LiteralExpression      TLiteralExpression        `xml:"literalExpression"`
	VersionTag             VersionTag                `xml:"extensionElements>versionTag"`
	InformationRequirement []TInformationRequirement `xml:"informationRequirement"`
}

type VersionTag struct {
	Value string `xml:"value,attr"`
}

type TDMNDI struct {
	DMNDiagrams []TDMNDiagram `xml:"dmndi:DMNDiagram"`
}

type TDMNDiagram struct {
	DMNShapes []TDMNShape `xml:"dmndi:DMNShape"`
}

type TDMNShape struct {
	Id            string  `xml:"id,attr"`
	DMNElementRef string  `xml:"dmnElementRef,attr"`
	Bounds        TBounds `xml:"dc:Bounds"`
}

type TBounds struct {
	Height float64 `xml:"height,attr"`
	Width  float64 `xml:"width,attr"`
	X      float64 `xml:"x,attr"`
	Y      float64 `xml:"y,attr"`
}

type TDecisionTable struct {
	Id                   string               `xml:"id,attr"`
	HitPolicy            HitPolicy            `xml:"hitPolicy,attr"`
	HitPolicyAggregation HitPolicyAggregation `xml:"aggregation,attr"`
	Inputs               []TInput             `xml:"input"`
	Outputs              []TOutput            `xml:"output"`
	Rules                []TRule              `xml:"rule"`
}

type TInput struct {
	Id              string           `xml:"id,attr"`
	Label           string           `xml:"label,attr"`
	InputExpression TInputExpression `xml:"inputExpression"`
}

type TInputExpression struct {
	Id      string  `xml:"id,attr"`
	TypeRef TypeRef `xml:"typeRef,attr"`
	Text    string  `xml:"text"`
}

type TOutput struct {
	Id      string  `xml:"id,attr"`
	Label   string  `xml:"label,attr"`
	Name    string  `xml:"name,attr"`
	TypeRef TypeRef `xml:"typeRef,attr"`
}

type TRule struct {
	Id          string         `xml:"id,attr"`
	InputEntry  []TInputEntry  `xml:"inputEntry"`
	OutputEntry []TOutputEntry `xml:"outputEntry"`
}

type TInputEntry struct {
	Id   string `xml:"id,attr"`
	Text string `xml:"text"`
}

type TOutputEntry struct {
	Id   string `xml:"id,attr"`
	Text string `xml:"text"`
}

type TVariable struct {
	Id      string  `xml:"id,attr"`
	Name    string  `xml:"name,attr"`
	TypeRef TypeRef `xml:"typeRef,attr"`
}

type TLiteralExpression struct {
	Id string `xml:"id,attr"`
}

type TInformationRequirement struct {
	Id               string            `xml:"id,attr"`
	RequiredDecision TRequiredDecision `xml:"requiredDecision"`
}

type TRequiredDecision struct {
	Href string `xml:"href,attr"`
}

type TypeRef string

const (
	TypeRefString            TypeRef = "string"
	TypeRefNumber            TypeRef = "number"
	TypeRefBoolean           TypeRef = "boolean"
	TypeRefDate              TypeRef = "date"
	TypeRefTime              TypeRef = "time"
	TypeRefDateTime          TypeRef = "dateTime"
	TypeRefDateTimeDuration  TypeRef = "dateTimeDuration"
	TypeRefYearMonthDuration TypeRef = "yearMonthDuration"
	TypeRefAny               TypeRef = "any"
)

type HitPolicy string

const (
	HitPolicyCollect     HitPolicy = "COLLECT"
	HitPolicyFirst       HitPolicy = "FIRST"
	HitPolicyPriority    HitPolicy = "PRIORITY"
	HitPolicyAny         HitPolicy = "ANY"
	HitPolicyRuleOrder   HitPolicy = "RULE ORDER"
	HitPolicyOutputOrder HitPolicy = "OUTPUT ORDER"
)

type HitPolicyAggregation string

const (
	HitPolicyAggregationSum   HitPolicyAggregation = "SUM"
	HitPolicyAggregationMin   HitPolicyAggregation = "MIN"
	HitPolicyAggregationMax   HitPolicyAggregation = "MAX"
	HitPolicyAggregationCount HitPolicyAggregation = "COUNT"
)
