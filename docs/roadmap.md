# Data Fabric Roadmap

**Last reviewed: 2026-10-15**

## Product direction

Data Fabric is a lightweight, self-hosted entry point for managing heterogeneous data-source connections and giving applications a consistent query API. It aims to reduce per-source integration and SQL-dialect work while keeping deployment and resource requirements small.

Federated cross-source execution is an extension direction. The current request path does not yet provide a federated query adapter. Roadmap items are plans, not claims of shipped capability or date commitments.

## How to read this roadmap

GitHub milestones are the source of truth for open work and issue status. Dates are omitted until scope, owners, and capacity are agreed. Later stages depend on evidence and completion gates from earlier stages. Review this roadmap when a milestone closes and at least quarterly.

## Stage 1 — Stabilize the gateway

**Status: in progress.** Tracked in [Core: Security and Reliability](https://github.com/RhinoLink-Tech/data-fabric/milestone/1).

Focus on security, query correctness, connector behavior, configuration, streaming lifecycle, observability, and deployment reliability.

Exit conditions:

- Critical security and data-loss risks are fixed or documented with mitigations.
- API behavior, authentication, configuration, and connector-support claims match the implementation.
- Query and streaming resources close reliably on success, cancellation, timeout, and failure.
- CI passes, and documented build and deployment paths are reproducible.
- Operators can diagnose health and connection failures without exposing credentials.

## Stage 2 — Add federated query-engine adapters

**Status: planned.** Tracked in [Phase 2: Compute Engine Integration](https://github.com/RhinoLink-Tech/data-fabric/milestone/2).

Define a small adapter contract, then prove the path from gateway configuration through engine connection, catalog selection, query execution, result retrieval, cancellation, and error handling. Start with one end-to-end verified engine before expanding to others.

Exit conditions:

- Engine configuration and credentials are validated and stored safely.
- An integration test demonstrates a cross-source query through the gateway.
- Cancellation, timeouts, authorization, errors, and result streaming behave consistently.
- The README distinguishes shipped adapters from future targets.

## Stage 3 — Operational readiness and ecosystem

**Status: candidate work; sequencing depends on Stages 1 and 2.** Related issues are grouped in [Phase 3](https://github.com/RhinoLink-Tech/data-fabric/milestone/3), [Phase 4](https://github.com/RhinoLink-Tech/data-fabric/milestone/4), and [Phase 5](https://github.com/RhinoLink-Tech/data-fabric/milestone/5).

Candidate areas include tenant and resource controls, metrics and auditability, verified connector coverage, deployment options, and community integrations. Analytics, ML, AI, and multi-cloud work should be added only for concrete user needs and a maintainable plan.

## Planning and completion rules

- Keep issues small enough to implement and review independently.
- Every issue should state the user problem, scope, and observable acceptance criteria.
- Add work to a milestone when sequencing is understood; use a triage label for unprioritized requests.
- Close issues only after acceptance criteria are met and behavior or limitations are documented.
- Rebaseline scope before assigning dates. Publish performance or availability targets only after measurement.
