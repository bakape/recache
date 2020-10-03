// parses benchmark log and prints its values in CSV format

package main

import (
	"bufio"
	"log"
	"os"
	"strconv"

	"golang.org/x/perf/benchstat"
)

func main() {
	c := &benchstat.Collection{
		Alpha:     0.05,
		DeltaTest: benchstat.UTest,
		Order:     benchstat.ByName,
	}

	f, err := os.Open(".bench_log")
	if err != nil {
		log.Fatal(err)
	}
	if err := c.AddFile(".bench_log", f); err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	var scratch []byte
	for k, v := range c.Metrics {
		w.WriteString(k.Benchmark)
		for _, v := range v.Values {
			w.WriteByte(',')
			scratch = strconv.AppendFloat(scratch[:0], v, 'f', 0, 64)
			w.Write(scratch)
		}
		w.WriteByte('\n')
	}
}
