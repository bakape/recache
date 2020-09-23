#! /bin/bash
set -e

rm -f .bench_log
for	i in {1..10}; do
	go test --bench . --benchtime 200x ./benchmarks/ >> .bench_log
done
benchstat --csv --sort name .bench_log > benchmark_results.csv
