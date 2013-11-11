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
	Label     string
}

func (self *Doc) Add(f *Service, t *Service, l string) {
	self.endPoints[fmt.Sprintf("%v_%v", f.Label, len(self.arrows))] = true
	self.endPoints[fmt.Sprintf("%v_%v", t.Label, len(self.arrows))] = true
	/*if len(self.arrows) > 1 {
		last := self.arrows[len(self.arrows)-1]
		if last.To != f {
			self.arrows = append(self.arrows, &Arrow{From: t, To: f})
		}
	}*/
	self.arrows = append(self.arrows, &Arrow{From: f, To: t, Label: l})

}

func (self *Doc) NewService(l string) *Service {
	s := &Service{Label: l}
	self.services = append(self.services, s)
	return s
}

func NewDoc(l string) *Doc {
	return &Doc{
		endPoints: map[string]bool{},
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
		fmt.Fprint(b, "}\n")
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

	fmt.Fprint(b, "}\n")
}
