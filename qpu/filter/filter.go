package filter

import (
	"fmt"

	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/fatih/color"
)

//Filter ...
func Filter(obj *pbQPU.Object, predicate []*pbQPU.Predicate) (bool, error) {
	for _, pred := range predicate {
		if obj.Attributes[pred.Attribute] < pred.Lbound || obj.Attributes[pred.Attribute] > pred.Ubound {

			//demo
			color.Set(color.FgRed)
			defer color.Unset()
			fmt.Println(obj)
			//

			return false, nil
		}
	}

	//demo
	color.Set(color.FgGreen)
	defer color.Unset()
	fmt.Println(obj)
	//

	return true, nil
}
