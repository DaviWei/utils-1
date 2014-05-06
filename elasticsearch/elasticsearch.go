package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/soundtrackyourbrand/utils/key"
)

type ElasticConnector interface {
	Client() *http.Client
	GetElasticService() string
	GetElasticUsername() string
	GetElasticPassword() string
}

var IndexNameProcessor = func(s string) string {
	return s
}

var legalizeRegexp = regexp.MustCompile("[^a-z0-9,]")

func processIndexName(s string) string {
	return IndexNameProcessor(legalizeRegexp.ReplaceAllString(strings.ToLower(s), ""))
}

type IndexOption string

const (
	AnalyzedIndex    IndexOption = "analyzed"
	NotAnalyzedIndex IndexOption = "not_analyzed"
	NoIndex          IndexOption = "no"
)

type Properties struct {
	Type   string                `json:"type"`
	Index  IndexOption           `json:"index,omitempty"`
	Store  bool                  `json:"store"`
	Fields map[string]Properties `json:"fields,omitempty"`
}

type DynamicTemplate struct {
	Match            string      `json:"match"`
	MatchMappingType string      `json:"match_mapping_type"`
	Mapping          *Properties `json:"mapping,omitempty"`
}

type Mapping struct {
	DynamicTemplates []map[string]DynamicTemplate `json:"dynamic_templates,omitempty"`
	Properties       map[string]Properties        `json:"properties,omitempty"`
}

type IndexDef struct {
	Mappings map[string]Mapping `json:"mappings,omitempty"`
	Template string             `json:"template,omitempty"`
}

func CreateIndex(c ElasticConnector, name string, def IndexDef) (err error) {
	return createIndexDef(c, "/"+processIndexName(name), def)
}

func createIndexDef(c ElasticConnector, path string, def IndexDef) (err error) {
	url := c.GetElasticService() + path
	b, err := json.Marshal(def)
	if err != nil {
		return
	}
	request, err := http.NewRequest("PUT", url, bytes.NewBuffer(b))
	if err != nil {
		return
	}
	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status trying to create index template in elasticsearch %v: %v", url, response.Status)
		return
	}
	return
}

func CreateIndexTemplate(c ElasticConnector, name string, def IndexDef) (err error) {
	return createIndexDef(c, "/_template/"+name, def)
}

/*
Clear will delete things.
If toDelete is empty, EVERYTHING will be deleted.
If toDelete has one element, that index will be deleted.
If toDelete has two elements, that index and doc type will be deleted.
*/
func Clear(c ElasticConnector, toDelete ...string) (err error) {
	url := c.GetElasticService()
	if len(toDelete) > 2 {
		err = fmt.Errorf("Can only give at most 2 string args to Clear")
		return
	} else if len(toDelete) == 2 {
		url += fmt.Sprintf("/%v/%v", processIndexName(toDelete[0]), toDelete[1])
	} else if len(toDelete) == 1 {
		url += fmt.Sprintf("/%v", processIndexName(toDelete[0]))
	} else {
		url += "/_all"
	}

	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return
	}
	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status trying to delete from elasticsearch %v: %v", url, response.Status)
		return
	}
	return
}

/*
CreateDynamicMapping will create a sane default dynamic mapping where all
string type fields are indexed twice, once analyzed under their proper name,
and once non-analyzed under [name].na
*/
func CreateDynamicMapping(c ElasticConnector) (err error) {
	indexDef := IndexDef{
		Template: "*",
		Mappings: map[string]Mapping{
			"_default_": Mapping{
				DynamicTemplates: []map[string]DynamicTemplate{
					map[string]DynamicTemplate{
						"default": DynamicTemplate{
							Match:            "*",
							MatchMappingType: "string",
							Mapping: &Properties{
								Type: "multi_field",
								Fields: map[string]Properties{
									"{name}": Properties{
										Index: AnalyzedIndex,
										Type:  "string",
										Store: true,
									},
									"{name}.na": Properties{
										Index: NotAnalyzedIndex,
										Type:  "string",
										Store: true,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	if err = CreateIndexTemplate(c, "default", indexDef); err != nil {
		return
	}
	return
}

func RemoveFromIndex(c ElasticConnector, index string, source interface{}) (err error) {
	index = processIndexName(index)
	value := reflect.ValueOf(source)
	id := value.Elem().FieldByName("Id").Interface().(key.Key).Encode()

	name := value.Elem().Type().Name()
	url := fmt.Sprintf("%s/%s/%s/%s",
		c.GetElasticService(),
		index,
		name,
		id)

	json, err := json.Marshal(source)
	if err != nil {
		return
	}
	request, err := http.NewRequest("DELETE", url, bytes.NewBuffer(json))
	if err != nil {
		return
	}

	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status code from elasticsearch %v: %v", url, response.Status)
		return
	}
	return
}

/*
AddToIndex adds source to a search index.
Source must have a field `Id *datastore.key`.
*/
func AddToIndex(c ElasticConnector, index string, source interface{}) (err error) {
	index = processIndexName(index)

	value := reflect.ValueOf(source)
	id := value.Elem().FieldByName("Id").Interface().(key.Key).Encode()

	name := value.Elem().Type().Name()

	json, err := json.Marshal(source)
	if err != nil {
		return
	}

	url := fmt.Sprintf("%s/%s/%s/%s",
		c.GetElasticService(),
		index,
		name,
		id)

	request, err := http.NewRequest("PUT", url, bytes.NewBuffer(json))
	if err != nil {
		return
	}

	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status code from elasticsearch %v: %v", url, response.Status)
		return
	}
	return
}

type PageableItems struct {
	Items []interface{}
	Total int
}

type StringQuery struct {
	Query           string `json:"query"`
	AnalyzeWildcard bool   `json:"analyze_wildcard"`
}

type Query struct {
	String   *StringQuery        `json:"query_string,omitempty"`
	Term     map[string]string   `json:"term,omitempty"`
	Range    map[string]RangeDef `json:"range,omitempty"`
	Bool     *BoolQuery          `json:"bool,omitempty"`
	Filtered *FilteredQuery      `json:"filtered,omitempty"`
	MatchAll *MatchAllQuery      `json:"match_all,omitempty"`
}

type MatchAllQuery struct {
	Boost float64 `json:"boost,omitempty"`
}

type SearchRequest struct {
	Query *Query      `json:"query,omitempty"`
	From  int         `json:"from,omitempty"`
	Size  int         `json:"size,omitempty"`
	Sort  interface{} `json:"sort,omitempty"`
}

type Sources []map[string]*json.RawMessage

type ElasticDoc struct {
	Index  string                      `json:"_index"`
	Type   string                      `json:"_type"`
	Id     string                      `json:"_id"`
	Score  float64                     `json:"_score"`
	Source map[string]*json.RawMessage `json:"_source"`
}

type Hits struct {
	Total    int          `json:"total"`
	MaxScore float64      `json:"max_score"`
	Hits     []ElasticDoc `json:"hits"`
}

type SearchResponse struct {
	Took float64 `json:"took"`
	Hits Hits    `json:"hits"`
}

type FilteredQuery struct {
	Query  *Query  `json:"query"`
	Filter *Filter `json:"filter"`
}

type BoolFilter struct {
	Must    []Filter `json:"must,omitempty"`
	MustNot []Filter `json:"must_not,omitempty"`
	Should  []Filter `json:"should,omitempty"`
}

type BoolQuery struct {
	Must               []Query `json:"must,omitempty"`
	MustNot            []Query `json:"must_not,omitempty"`
	Should             []Query `json:"should,omitempty"`
	MinimumShouldMatch int     `json:"minimum_should_match,omitempty"`
	Boost              float64 `json:"boost,omitempty"`
}

type Filter struct {
	Or    []Query             `json:"or,omitempty"`
	Query *Query              `json:"query,omitempty"`
	Bool  *BoolFilter         `json:"bool,omitempty"`
	Term  map[string]string   `json:"term,omitempty"`
	Range map[string]RangeDef `json:"range,omitempty"`
}

type RangeDef struct {
	Gt    string `json:"gt,omitempty"`
	Gte   string `json:"gte,omitempty"`
	Lt    string `json:"lt,omitempty"`
	Lte   string `json:"lte,omitempty"`
	Boost string `json:"boost,omitempty"`
}

/*
Search will run the queryString (a query string parseable by http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)
(or elasticQuery if provided, a JSON string describing a request body according to http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-body.html)
sorting it using the specified sort (a JSON string describing a sort according to http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-sort.html),
and limiting/offsetting it using the provided limit and offset.
*/
func Search(c ElasticConnector, query *SearchRequest, index string, result interface{}) (err error) {
	index = processIndexName(index)

	resultValue := reflect.ValueOf(result).Elem()
	resultItems := resultValue.FieldByName("Items")

	name := resultItems.Type().Elem().Name()

	url := c.GetElasticService()
	if index == "" {
		url += "/_all"
	} else {
		url += "/" + index
	}
	if name != "" {
		url += "/" + name
	}
	url += "/_search"

	b, err := json.Marshal(query)
	if err != nil {
		return
	}

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		return
	}

	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}

	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status trying to search in elasticsearch %v: %v", url, response.Status)
		return
	}

	elasticResult := &SearchResponse{}
	err = json.NewDecoder(response.Body).Decode(&elasticResult)
	if err != nil {
		return
	}

	sources := make(Sources, len(elasticResult.Hits.Hits))
	for index, hit := range elasticResult.Hits.Hits {
		sources[index] = hit.Source
	}
	buf, err := json.Marshal(sources)
	if err != nil {
		return
	}
	if err = json.Unmarshal(buf, resultItems.Addr().Interface()); err != nil {
		return
	}
	resultValue.FieldByName("Total").Set(reflect.ValueOf(elasticResult.Hits.Total))

	return
}