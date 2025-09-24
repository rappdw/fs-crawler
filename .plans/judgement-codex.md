# Judgement: Codex vs Claude Refactor Plans

## Prompt Alignment Overview
- **Core objective**: Enable `fs-crawler` to harvest a very large share of FamilySearch parent/child relationships while persisting progress, staying within memory limits, remaining polite to the API, and supporting pause/resume.
- **Success criteria**: Durable storage for graph and frontier, resumable crawl orchestration, adaptive rate limiting, and operational guardrails for long runs.

## Evaluation of `refactor-codex.md`
- **Clarity & structure**: Nine phases grouped by capability (persistence, frontier durability, checkpointing, throttling, pause/cancel, observability, testing, documentation, and code-quality). Each phase maps directly to a prompt requirement, making prioritisation straightforward.
- **Strengths**:
  - Explicit move to on-disk SQLite with WAL plus incremental commits, directly addressing memory pressure and crash safety.
  - Dedicated steps for disk-backed frontier/visited tracking and checkpoint semantics, ensuring true suspend/resume.
  - Polite crawling receives focused treatment (shared throttling configuration, adaptive backoff, signal handling for graceful pauses).
  - Observability and testing extensions scoped to the new persistence and resume paths.
- **Limitations**:
  - Leaves authentication/token lifecycle implicit; long crawls may require explicit refresh/rotation planning.
  - CLI surface for pause/resume is mentioned but lacks operator workflow detail (e.g., how status is surfaced besides CLI commands).
  - Success metrics are implied (baseline measurement) but not carried through later phases.

## Evaluation of `refactor-claude.md`
- **Clarity & structure**: Seven phases with numerous subcomponents, effort/risk tags, timelines, and ancillary features (distributed crawling, advanced metrics, export formats).
- **Strengths**:
  - Rich implementation detail (new modules, risk mitigation, success metrics) that could inform execution once scope is locked.
  - Comprehensive treatment of monitoring, error handling, and quality gates.
- **Limitations**:
  - Scope creep beyond the prompt (distributed crawling, advanced export formats, circuit breakers, health endpoints) which dilutes focus and elevates risk.
  - Multiple overlapping components (request queue manager, crawl manager, strategies) without clear sequencing minimal slices.
  - Heavy emphasis on new subsystems before confirming baseline durability needs; increases time to first reversible milestone.
  - Some tasks (health endpoints, chaos testing, partitioning) assume infrastructure not required for the immediate objective.

## Comparative Analysis
- **Alignment with prompt**: Codex maintains a tight focus on persistence, resumability, and rate limiting. Claude plan devotes significant effort to supplementary features that the prompt does not mandate.
- **Deliverability**: Codex phases are concise and can be executed incrementally, lowering risk of regressions. Claude phases introduce large refactors (new managers, strategies) that may destabilize the existing crawler before core needs are met.
- **Operational readiness**: Both plans acknowledge monitoring and documentation, but Codex ties them to crawl durability checkpoints, whereas Claude defers key pause/resume guarantees behind broader orchestration changes.
- **Resource efficiency**: Codex minimises new abstractions, reusing the current architecture where possible. Claude plan proliferates new modules and directories, raising maintenance overhead.

## Recommendation
- **Preferred plan**: `refactor-codex.md`
- **Rationale**: It is better expressed as a sequence of actionable, prompt-focused milestones that directly mitigate the crawler's known pain points (persistent graph storage, durable frontier, graceful shutdown, adaptive throttling). The tighter scope and emphasis on incremental durability improvements make it more likely to achieve the objective quickly while keeping risk manageable. Elements from `refactor-claude.md` (e.g., explicit risk tracking, success metrics, richer logging) can be harvested selectively during execution.

## Follow-up Suggestions
- **Incorporate targeted improvements**: Blend in Claude's success-metric tracking and risk mitigation language to ensure accountability without expanding scope.
- **Clarify operator workflows**: Document how the CLI checkpoints and status inspection work alongside the graceful shutdown hooks described in `refactor-codex.md`.
