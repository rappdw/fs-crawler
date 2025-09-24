# Supplemental Enhancements for `refactor-codex.md`

1. **Token & Session Lifecycle Management**
   - Document and implement automatic refresh for OAuth/session tokens to ensure multi-day crawls remain authenticated.
   - Add telemetry for upcoming token expiry and error handling for forced re-login scenarios.

2. **Operator Workflow Clarification**
   - Expand CLI/user documentation describing how checkpoint creation, status inspection, pause, and resume commands work together.
   - Provide example runbooks for pausing via signals vs explicit CLI commands, including expected log output.

3. **Success Metrics Tracking**
   - Carry forward the baseline measurements collected in Phase 1 to define concrete acceptance metrics (e.g., sustained requests/sec under throttle, checkpoint recovery time).
   - Add lightweight metric recording and reporting hooks (log summary or CSV export) to verify targets during test crawls.
