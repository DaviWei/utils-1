package seqdiag

import (
	"fmt"
	"io"
)

type Service struct {
	Doc   *Doc
	Label string
}

type Arrow struct {
	From  *Service
	To    *Service
	Label string
}

type Doc struct {
	arrows    []*Arrow
	services  []*Service
	notes     map[string]string
	endPoints map[string]bool
	Label     string
}

func (f *Service) Add(t *Service, l string) *Service {
	doc := f.Doc
	doc.endPoints[fmt.Sprintf("%v_%v", f.Label, len(doc.arrows))] = true
	doc.endPoints[fmt.Sprintf("%v_%v", t.Label, len(doc.arrows))] = true
	/*if len(doc.arrows) > 1 {
		last := doc.arrows[len(doc.arrows)-1]
		if last.To != f {
			doc.arrows = append(doc.arrows, &Arrow{From: t, To: f})
		}
	}*/
	doc.arrows = append(doc.arrows, &Arrow{From: f, To: t, Label: l})
	return t
}

func (self *Service) AddNote(note string) *Service {
	key := fmt.Sprintf("Info%d", len(self.Doc.arrows)-1)
	self.Doc.endPoints[key] = true
	self.Doc.notes[key] = note
	return self
}

func (self *Doc) NewService(l string) *Service {
	s := &Service{Label: l, Doc: self}
	self.services = append(self.services, s)
	return s
}

func NewDoc(l string) *Doc {
	return &Doc{
		endPoints: map[string]bool{},
		notes:     map[string]string{},
		Label:     l,
	}
}

func (self *Doc) Generate(b io.Writer) {
	fmt.Fprintf(b, `
digraph %s {
	ranksep=.3; size = "7.5,7.5";
	node [fontsize=10, shape=point, color=grey,  label=""];
	edge [arrowhead=none, style=filled, color=lightgray];
`, self.Label)

	// Plot headers.
	for i := 0; i < len(self.services)-1; i++ {
		fmt.Fprintf(b, "\t%s -> %s [style=invis]\n", self.services[i].Label, self.services[i+1].Label)
	}

	// Plot vertical lines
	for _, service := range self.services {

		fmt.Fprintf(b, "\n\n\t%v [color=black, shape=box, label=\"%v\"];\n",
			service.Label, service.Label)
		last := service.Label
		for i := 0; i < len(self.arrows); i++ {
			key := fmt.Sprintf("%v_%v", service.Label, i)
			if self.endPoints[key] {
				fmt.Fprintf(b, "\t%v -> %v;\n", last, key)
				last = key
			}
		}
		fmt.Fprintf(b, "\t%v -> %s_footer;\n", last, service.Label)
	}

	// Rank header
	fmt.Fprint(b, "\n\n\t{ rank = same; ")
	for _, service := range self.services {
		fmt.Fprintf(b, "%s;\t", service.Label)
	}
	fmt.Fprint(b, "}\n")

	// Rank content
	for i := 0; i < len(self.arrows); i++ {
		fmt.Fprint(b, "\t{ rank = same; ")
		for _, service := range self.services {
			key := fmt.Sprintf("%v_%v", service.Label, i)
			if self.endPoints[key] {
				fmt.Fprintf(b, "%v;\t", key)
			}
		}
		if self.endPoints[fmt.Sprintf("Info%d", i)] {
			fmt.Fprintf(b, "Info%d;", i)
		}
		fmt.Fprintf(b, "}\n")
	}

	// Rank footer
	fmt.Fprint(b, "\t{ rank = same; ")
	for _, service := range self.services {
		fmt.Fprintf(b, "%s_footer;\t", service.Label)
	}
	fmt.Fprint(b, "}\n")

	// Print arrows
	fmt.Fprint(b, "\n\tedge [constraint=false, style=filled, fontsize=8, weight=0, arrowtail=none, arrowhead=normal, color=green];\n")

	for i, arrow := range self.arrows {
		if arrow.Label != "" {
			fmt.Fprintf(b, "\t%s_%d -> %s_%d [label=\"%s\"];\n", arrow.From.Label, i, arrow.To.Label, i, arrow.Label)
		} else {
			fmt.Fprintf(b, "\t%s_%d -> %s_%d;\n", arrow.From.Label, i, arrow.To.Label, i)
		}
	}

	for k, v := range self.notes {
		fmt.Fprintf(b, "%s [color=black, shape=larrow, width=1.5, label=\"%s\"];\n", k, v)
	}

	fmt.Fprint(b, "}\n")
}
