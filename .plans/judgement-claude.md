# Comparative Judgement: `refactor-codex.md` vs `refactor-claude.md`

## Prompt-Aligned Objectives
- **Scalability**: Persist graph and frontier data beyond memory limits.
- **Resumability**: Support safe suspend, resume, and crash recovery.
- **Polite API Usage**: Enforce respectful, configurable rate limiting and backoff.
- **Operational Robustness**: Provide observability, testing, and documentation sufficient for long-running crawls.

## Evaluation Criteria
- **Objective Coverage**: Directness in addressing the prompt's four core needs.
- **Scope Control**: Focus on necessary capabilities without diluting effort.
- **Implementation Sequencing**: Clear ordering that reduces risk and delivers value incrementally.
- **Operational Readiness**: Attention to monitoring, testing, and operator workflows.

## Assessment of `refactor-codex.md`
- **Strengths**
  - **Laser focus on persistence and resumability** via durable storage, checkpointing, and disk-backed frontier queues (`Persistent Storage Refactor`, `Durable Frontier & Visited Tracking`, `Checkpointing & Resume Semantics`).
  - **Polite request management** coupled with signal-based pause/cancel ensures API friendliness and graceful shutdowns.
  - **Operational hygiene** through observability, testing, and documentation phases tightly linked to the new persistence model.
  - **Pragmatic sequencing**: establishing baseline metrics before refactor, then layering durability, throttling, and tooling in a logical order.
- **Gaps / Risks**
  - Configuration flexibility is implied but not explicit (could benefit from a dedicated configuration governance step).
  - Testing section could explicitly mention simulated failure/restart drills to validate resumability claims.

## Assessment of `refactor-claude.md`
- **Strengths**
  - **Comprehensive catalog** spanning database, state management, rate limiting, monitoring, error handling, and performance optimization.
  - **Risk assessment and timelines** help communicate complexity and sequencing at a program-management level.
  - **Attention to tooling** (configuration management, test suite, documentation) aligns with long-term maintainability.
- **Gaps / Risks**
  - **Scope creep beyond prompt**: distributed crawling, advanced strategies, export pipelines, and circuit breaker infrastructure dilute focus from the immediate persistence/resume challenge.
  - **Ambiguity in critical details**: durable frontier storage, checkpoint schema, and crash recovery semantics are mentioned but less concretely specified than in the Codex plan.
  - **High implementation surface area** may delay delivery of the essential durability features the prompt emphasizes.

## Comparative Analysis
- **Alignment with Prompt**: The Codex plan prioritizes persistence, resumability, and throttling as first-order concerns with concrete steps; the Claude plan intermixes these with later-phase feature expansions that could postpone the core deliverables.
- **Clarity of Execution**: Codex's roadmap is concise and actionable for the existing codebase, whereas Claude's plan requires substantial new subsystems (e.g., `crawl_manager`, `monitoring/` package) which increase ramp-up overhead.
- **Risk Management**: Claude's inclusion of high-risk, non-essential features (e.g., distributed crawling) before validating core durability increases schedule risk; Codex mitigates risk by establishing baselines, implementing persistence, then layering operational controls.

## Recommendation
**`refactor-codex.md` best satisfies the prompt.** It provides a tight, goal-driven roadmap that directly addresses scalable persistence, graceful resume, and polite rate limiting while maintaining manageable scope. Adopting its structure—with minor enhancements for configuration governance and restart testing—will deliver the required capabilities faster and with lower risk.
