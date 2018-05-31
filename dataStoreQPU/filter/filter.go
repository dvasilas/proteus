package filter

import (
	"fmt"
	"time"

	pbDS "github.com/dimitriosvasilas/modqp/dataStore/datastore"
	pbDSQU "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpu"
	"github.com/fatih/color"
)

//Filter ...
func Filter(obj *pbDS.ObjectMD, predicate []*pbDSQU.Predicate) (bool, error) {
	//demo
	time.Sleep(time.Second / 4)
	//
	for _, pred := range predicate {
		if obj.State[pred.Attribute] < pred.Lbound || obj.State[pred.Attribute] > pred.Ubound {

			//demo
			color.Set(color.FgRed)
			fmt.Println(obj)
			defer color.Unset()
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
