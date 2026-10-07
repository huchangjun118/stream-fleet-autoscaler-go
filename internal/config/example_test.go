package config
import "testing"
func TestExampleLoads(t *testing.T){
 c,err:=LoadFile("../../configs/example.json")
 if err!=nil{t.Fatal(err)}
 if c.MaxReplicas!=12 || c.Specs["large"]!=25{t.Fatalf("bad parse %+v",c)}
}
