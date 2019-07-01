package mapindex

import (
	"fmt"
	"os"
	"testing"

	"github.com/dvasilas/proteus/src/protos"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/dvasilas/proteus/src/testUtils"
)

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

func TestEncodeDecode(t *testing.T) {
	obj := testutils.ObjectState("key", "buck", "counter", pbUtils.Attribute_CRDTCOUNTER, protoutils.ValueInt(42))

	b, err := encodeIndexEntry(obj)

	fmt.Println(err)
	fmt.Println(string(b))

	objdecoded, err := decodeIndexEntry(b)
	fmt.Println(err)
	fmt.Println(objdecoded)
}
