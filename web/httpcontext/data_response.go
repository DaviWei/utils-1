package httpcontext

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
)

const (
	ContentJSON     = "application/json; charset=UTF-8"
	ContentExcelCSV = "application/vnd.ms-excel"
)

type DataResp struct {
	Body        []map[string]interface{}
	Status      int
	ContentType string
	Filename    string
}

func (self DataResp) Write(w http.ResponseWriter) error {
	if self.Body == nil {
		return nil
	}
	if self.Filename != "" {
		w.Header().Set("Content-disposition", "attachment; filename="+self.Filename)
	}
	w.Header().Set("Content-Type", self.ContentType)
	switch self.ContentType {
	case ContentExcelCSV:
		if self.Status != 0 {
			w.WriteHeader(self.Status)
		}
		writer := csv.NewWriter(w)
		writer.Comma = '\t'
		var keys []string = nil
		for _, row := range self.Body {
			if keys == nil {
				keys = make([]string, 0, len(row))
				for k := range row {
					keys = append(keys, k)
				}
				err := writer.Write(keys)
				if err != nil {
					return err
				}
			}
			vals := make([]string, 0, len(keys))
			for _, v := range keys {
				vals = append(vals, fmt.Sprintf("%v", row[v]))
			}
			err := writer.Write(vals)
			if err != nil {
				return err
			}
		}
		writer.Flush()
		return writer.Error()
	case ContentJSON:
		return json.NewEncoder(w).Encode(self.Body)
	}
	return fmt.Errorf("Unknown content type %#v", self.ContentType)
}

var suffixPattern = regexp.MustCompile("\\.(\\w{1,4})$")

func DataHandlerFunc(f func(c HTTPContextLogger) (result DataResp, err error)) http.Handler {
	return HandlerFunc(func(c HTTPContextLogger) (err error) {
		resp, err := f(c)
		match := suffixPattern.FindStringSubmatch(c.Req().URL.Path)
		suffix := ""
		if match != nil {
			suffix = match[1]
		}
		switch suffix {
		case "csv":
			resp.ContentType = ContentExcelCSV
		default:
			resp.ContentType = ContentJSON
		}
		if err == nil {
			c.Render(resp)
		}
		return
	})
}
