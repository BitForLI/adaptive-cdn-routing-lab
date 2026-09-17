# Evidence-backed resume bullets

Project title:

```text
EdgeRoute — QoE-Aware Traffic Steering for Distributed Edge Caches
```

Technology:

```text
Go, CoreDNS, Kubernetes, Prometheus, NGINX, MediaMTX, Toxiproxy, k6, Docker, Helm
```

Bullets supported by the committed repository and recorded results:

```text
• Extended EdgeCDN-X's Go/CoreDNS plugin with a Kubernetes NodeQuality
  controller that converts Prometheus latency, error, cache, origin and
  capacity telemetry into bounded per-node routing weights outside the DNS
  request path.

• Implemented EWMA smoothing, consecutive/error-rate outlier ejection,
  per-location ejection limits, last-known-good stale handling, weighted
  rendezvous selection and staged 10%/25%/50%/100% recovery state factors while
  preserving upstream geographic fallback.

• Built a reproducible HLS fault lab from MediaMTX, three NGINX caches,
  Toxiproxy and k6; a 36-run smoke matrix recorded 0% session failures for
  both static-rendezvous and adaptive routing during connection reset and
  cache Pod outage, versus 56.18% and 55.52% for the deterministic control.

• Added unit, race, fuzz, distribution and end-to-end checks plus strict
  Git/image/Job/Pod/run identity validation; measured the isolated 512-candidate
  weighted-selection function at 35.51 us/op with 0 allocations on an Intel
  i5-12400F Windows/amd64 lab host.
```

Traceability:

| Claim | Evidence |
|---|---|
| 36 runs and three repetitions | `experiments/results/processed/report.md`, `runs.csv` |
| 56.18% → 0% disconnect | generated report, raw `*/disconnect-*/k6-summary.json` |
| 55.52% → 0% Pod outage | generated report, raw `*/pod-down-*/k6-summary.json` |
| Isolated 512-candidate selection, 35.51 us/op, 0 alloc | `docs/algorithm.md`, `internal/routing/rendezvous_benchmark_test.go` |
| unit/race/fuzz/e2e | test sources, CI workflow, and Day 7 verification record |

Do not rewrite the bullets as “built a global CDN”, “production SLO”, “100 VU performance improvement”, “AI scheduling”, or “improved cache efficiency”. The experiment is a one-workstation smoke profile; the static-rendezvous control shows that active-health/fallback and hash-family effects share credit for the observed fault improvements.
