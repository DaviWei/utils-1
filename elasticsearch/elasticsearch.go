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

var legalizeRegexp = regexp.MustCompile("[^a-z0-9,]")

func legalizeIndexName(s string) string {
	return legalizeRegexp.ReplaceAllString(strings.ToLower(s), "")
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
		url += fmt.Sprintf("/%v/%v", legalizeIndexName(toDelete[0]), toDelete[1])
	} else if len(toDelete) == 1 {
		url += fmt.Sprintf("/%v", legalizeIndexName(toDelete[0]))
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

func RemoveFromIndex(c ElasticConnector, index string, source interface{}) (err error) {
	index = legalizeIndexName(index)
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
	index = legalizeIndexName(index)

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

type ElasticStringQuery struct {
	Query           string `json:"query"`
	AnalyzeWildcard bool   `json:"analyze_wildcard"`
}

type ElasticQuery struct {
	StringQuery *ElasticStringQuery `json:"query_string,omitempty"`
}

type ElasticSearchRequest struct {
	Query interface{} `json:"query,omitempty"`
	From  int         `json:"from,omitempty"`
	Size  int         `json:"size,omitempty"`
	Sort  interface{} `json:"sort,omitempty"`
}

type ElasticSources []map[string]*json.RawMessage

type ElasticDoc struct {
	Index  string                      `json:"_index"`
	Type   string                      `json:"_type"`
	Id     string                      `json:"_id"`
	Score  float64                     `json:"_score"`
	Source map[string]*json.RawMessage `json:"_source"`
}

type ElasticHits struct {
	Total    int          `json:"total"`
	MaxScore float64      `json:"max_score"`
	Hits     []ElasticDoc `json:"hits"`
}

type ElasticResponse struct {
	Took float64     `json:"took"`
	Hits ElasticHits `json:"hits"`
}

/*
Search will run the queryString (a query string parseable by http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)
(or elasticQuery if provided, a JSON string describing a request body according to http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-body.html)
sorting it using the specified sort (a JSON string describing a sort according to http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-sort.html),
and limiting/offsetting it using the provided limit and offset.
*/
func Search(c ElasticConnector, query *ElasticSearchRequest, index string, result interface{}) (err error) {
	index = legalizeIndexName(index)

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

	elasticResult := &ElasticResponse{}
	err = json.NewDecoder(response.Body).Decode(&elasticResult)
	if err != nil {
		return
	}

	sources := make(ElasticSources, len(elasticResult.Hits.Hits))
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
