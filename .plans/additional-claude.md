# Supplemental Adjustments for `refactor-claude.md`

- **Focus Scope on Core Prompt Needs**  
  Defer distributed crawling, advanced strategies, and export integrations until after persistence, resumability, and throttling are proven in production crawl drills.

- **Define Durable Frontier & Checkpoint Schema**  
  Introduce explicit designs for disk-backed frontier queues, visited tracking, and checkpoint metadata with table layouts and integrity constraints to guarantee restart fidelity.

- **Checkpoint Validation Test Suite**  
  Add automated failure-injection tests that simulate mid-crawl termination, signal-triggered pauses, and restart sequencing to ensure resumability is verifiable.

- **Configuration & CLI Cohesion**  
  Replace the new `crawl_manager`/config primitives with a focused CLI configuration surface (flags + config file) that directly tunes persistence paths, throttle ceilings, and checkpoint cadence.

- **Operational Success Metrics**  
  Carry baseline metrics forward into acceptance criteria covering sustained crawl throughput, checkpoint recovery time, and API error budgets to keep implementation grounded in measurable outcomes.
