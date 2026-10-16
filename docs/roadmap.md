# Data Fabric Roadmap

**Last reviewed: 2026-10-16**

## Product direction

Data Fabric is a lightweight, self-hosted entry point for managing heterogeneous data-source connections and giving applications a consistent query API. It supports direct single-source access through gateway drivers and optional federated execution through external query-engine adapters. The product focuses on a small deployment footprint, unified source management, and a bounded, testable SQL compatibility contract.

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

## Stage 2 — Federated query-engine adapter

**Adapter track status: delivered and protocol-level regression verified.** Tracked in [Phase 2: Compute Engine Integration](https://github.com/RhinoLink-Tech/data-fabric/milestone/2). The milestone also contains separate compute and operations work.

Data Fabric can register a Trino, Presto, or OpenLooKeng coordinator as a managed data source and route SQL through the shared query-engine adapter. The external engine owns catalog configuration, connector access, cross-catalog planning, and execution.

Delivered work:

- [#160](https://github.com/RhinoLink-Tech/data-fabric/issues/160): Go-compatible federation adapter contract.
- [#161](https://github.com/RhinoLink-Tech/data-fabric/issues/161): endpoint configuration and validation.
- [#162](https://github.com/RhinoLink-Tech/data-fabric/issues/162): query routing through the adapter and normalized results.
- [#163](https://github.com/RhinoLink-Tech/data-fabric/issues/163): timeout and cancellation propagation.
- [#164](https://github.com/RhinoLink-Tech/data-fabric/issues/164): simulated-coordinator integration coverage for a query referencing multiple catalogs, request routing, and returned columns and rows.
- [#165](https://github.com/RhinoLink-Tech/data-fabric/issues/165): SQL v1 dialect regression coverage.

The protocol-level integration suite uses a deterministic simulated coordinator so it runs in regular CI without external services. It verifies the gateway-to-engine request and result path. Engine-side catalog behavior, connector support, permissions, and version-specific SQL remain determined by the deployed engine.

## Stage 3 — Operational readiness and ecosystem

**Status: candidate work; sequencing depends on Stage 1 and Stage 2.** Related issues are grouped in [Phase 3](https://github.com/RhinoLink-Tech/data-fabric/milestone/3), [Phase 4](https://github.com/RhinoLink-Tech/data-fabric/milestone/4), and [Phase 5](https://github.com/RhinoLink-Tech/data-fabric/milestone/5).

Candidate areas include tenant and resource controls, metrics and auditability, verified connector coverage, deployment options, and community integrations. Analytics, ML, AI, and multi-cloud work should be added only for concrete user needs and a maintainable plan.

## Planning and completion rules

- Keep issues small enough to implement and review independently.
- Every issue should state the user problem, scope, and observable acceptance criteria.
- Add work to a milestone when sequencing is understood; use a triage label for unprioritized requests.
- Close issues only after acceptance criteria are met and behavior or limitations are documented.
- Rebaseline scope before assigning dates. Publish performance or availability targets only after measurement.
