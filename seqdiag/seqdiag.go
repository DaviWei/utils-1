package seqdiag

import (
	"fmt"
	"io"
)

type Service struct {
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
	endPoints map[string]bool
}

func (self *Doc) Add(f *Service, t *Service, l string) {
	self.endPoints[fmt.Sprintf("%v_%v", f.Label, len(self.arrows))] = true
	self.endPoints[fmt.Sprintf("%v_%v", t.Label, len(self.arrows))] = true
	self.arrows = append(self.arrows, &Arrow{From: f, To: t, Label: l})
}

func (self *Doc) NewService(l string) *Service {
	s := &Service{Label: l}
	self.services = append(self.services, s)
	return s
}

func NewDoc() *Doc {
	return &Doc{
		endPoints: map[string]bool{},
	}
}

func (self *Doc) Generate(b io.Writer) {
	fmt.Fprint(b, `
digraph G{
	ranksep=.1; size = "7.5,7.5";
	node [fontsize=10, shape=point, color=grey,  label=""];
	edge [arrowhead=none, style=filled, color=grey];
`)
	for i := 0; i < len(self.services)-1; i++ {
		fmt.Fprintf(b, "\t%s -> %s [style=invis]\n", self.services[i].Label, self.services[i+1].Label)
	}

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
	for i := 0; i < len(self.services); i++ {
		fmt.Fprint(b, "\t{ rank = same; ")
		for _, service := range self.services {
			key := fmt.Sprintf("%v_%v", service.Label, i)
			if self.endPoints[key] {
				fmt.Fprintf(b, "%v;\t", key)
			}
		}
		fmt.Fprint(b, "}\n")
	}

	// Rank footer
	fmt.Fprint(b, "\t{ rank = same; ")
	for _, service := range self.services {
		fmt.Fprintf(b, "%s_footer;\t", service.Label)
	}
	fmt.Fprint(b, "}\n")

	fmt.Fprint(b, "\n\tedge [style=filled, fontsize=8, weight=0, arrowtail=none, arrowhead=normal, color=black];\n")

	for i, arrow := range self.arrows {
		fmt.Fprintf(b, "\t%s_%d -> %s_%d [label=\"%s\"];\n", arrow.From.Label, i, arrow.To.Label, i, arrow.Label)
	}

	fmt.Fprint(b, "}\n")
}
