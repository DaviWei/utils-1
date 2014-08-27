package bigquery

import (
	gbigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"net/http"
)

const (
	iss       = "syb-core-development-warehouse@appspot.gserviceaccount.com"
	projectId = "syb-core-development-warehouse"
	datasetId = "test_dataset" //"warehouse"
)

type BigQuery struct {
	service *gbigquery.Service
}

func New(client *http.Client) (result *BigQuery, err error) {
	service, err := gbigquery.New(client)
	if err != nil {
		return
	}
	result = &BigQuery{
		service: service,
	}
	return
}

/*
AssertTable will check if a table named after i exists.
If it does, it will patch it so that it has all missing columns.
If it does not, it will create it.
Then it will check if there exists a view of the same table that only shows
the latest (counted by UpdatedAt) row per unique Id.
It assumes that i has a field "Id" that is a key.Key, and a field "UpdatedAt" that is a utils.Time.
*/
func (self *BigQuery) AssertTable(i interface{}) (err error) {
	table := &gbigquery.Table{}
	table.TableReference = &gbigquery.TableReference{
		DatasetId: datasetId,
		ProjectId: projectId,
		TableId:   "test_data",
	}
	job := &gbigquery.Job{
		Configuration: &gbigquery.JobConfiguration{
			Load: &gbigquery.JobConfigurationLoad{
				DestinationTable: &gbigquery.TableReference{
					DatasetId: datasetId,
					ProjectId: projectId,
					TableId:   table.TableReference.TableId,
				},
				//MaxBadRecords:    source.maxBadRecords,
				//Schema:           &source.schema,
				//SourceUris:       []string{source.uri},
				//WriteDisposition: source.disposition,
			},
		},
	}

	call := self.service.Jobs.Insert(projectId, job)
	job, err = call.Do()
	if err != nil {
		return err
	}

	return
}
