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
	ContentHTML     = "text/html"
)

type DataResp struct {
	Data        chan []interface{}
	Headers     []string
	Status      int
	ContentType string
	Filename    string
}

type JsonResp struct {
	Data    [][]interface{} `json:"data"`
	Headers []string        `json:"headers"`
}

func (self DataResp) Write(w http.ResponseWriter) error {
	if self.Data == nil {
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
		fmt.Fprintf(w, "sep=\t\n")
		writer := csv.NewWriter(w)
		writer.Comma = '\t'
		err := writer.Write(self.Headers)
		if err != nil {
			return err
		}
		for row := range self.Data {
			vals := make([]string, 0, len(self.Headers))
			for index := range self.Headers {
				vals = append(vals, fmt.Sprintf("%v", row[index]))
			}
			err := writer.Write(vals)
			if err != nil {
				return err
			}
		}
		writer.Flush()
		return writer.Error()
	case ContentHTML:
		fmt.Fprintf(w, "<html><body><table><thead><tr>")
		for _, k := range self.Headers {
			fmt.Fprintf(w, "<th>%v</th>", k)
		}
		fmt.Fprintf(w, "</tr></thead><tbody>")
		for row := range self.Data {
			fmt.Fprintf(w, "<tr>")
			for _, v := range row {
				fmt.Fprintf(w, "<td>%v</td>", v)
			}
			fmt.Fprintf(w, "</tr>")
		}
		fmt.Fprintf(w, "</tbody></body></html>")
	case ContentJSON:
		// I dont know a way of creating json, and streaming it to the user.
		resp := JsonResp{}
		resp.Headers = self.Headers
		for row := range self.Data {
			resp.Data = append(resp.Data, row)
		}
		return json.NewEncoder(w).Encode(resp)
	}
	return fmt.Errorf("Unknown content type %#v", self.ContentType)
}

var suffixPattern = regexp.MustCompile("\\.(\\w{1,4})$")

func DataHandlerFunc(f func(c HTTPContextLogger) (result *DataResp, err error)) http.Handler {
	return HandlerFunc(func(c HTTPContextLogger) (err error) {
		resp, err := f(c)
		if err != nil {
			return
		}
		match := suffixPattern.FindStringSubmatch(c.Req().URL.Path)
		suffix := ""
		if match != nil {
			suffix = match[1]
		}
		switch suffix {
		case "csv":
			resp.ContentType = ContentExcelCSV
		case "html":
			resp.ContentType = ContentHTML
		default:
			resp.ContentType = ContentJSON
		}
		if err == nil {
			c.Render(resp)
		}
		return
	})
}
