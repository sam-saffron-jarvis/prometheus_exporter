# Transport benchmark

Run the finite-request end-to-end benchmark from a clean checkout with:

```sh
bundle install
RECORDS=10000 RUNS=3 bundle exec ruby -Ilib bench/bench.rb
```

It reports enqueue time and enqueue-to-final-client-acknowledgement time for each run. The timer stops only after the client has read the final HTTP response, not when the collector first sees the record. Each record is JSON-serialized, sent as its own finite HTTP request, acknowledged, and processed by the collector. The client reuses its persistent HTTP/1.1 connection when `Net::HTTP` determines that reuse is safe. The server asks the operating system for a port by binding directly to port `0`.

For a reproducible comparison:

1. Use separate worktrees for the revisions being compared.
2. Run the same Ruby version, dependency lock, `RECORDS`, and `RUNS` in each worktree.
3. Run on the same otherwise-idle host; record CPU, operating system, Ruby, Puma, TLS, and network setup.
4. Include warm-up runs, then report every measured run and the aggregation method rather than selecting a single result.
5. For cross-protocol comparisons, use a benchmark driver appropriate to each revision and keep the generated record and collector work equivalent.

A quick smoke run is:

```sh
RECORDS=10 RUNS=1 bundle exec ruby -Ilib bench/bench.rb
```

The benchmark is procedural and intentionally contains no checked-in machine-specific throughput or performance claims.
