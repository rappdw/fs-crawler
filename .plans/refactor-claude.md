# FamilySearch Crawler Refactoring Plan

## Executive Summary

This plan outlines the refactoring of the fs-crawler to support large-scale extraction of parent-child relationships from FamilySearch.org. The current implementation has good foundations but needs enhancements for scalability, persistence, rate limiting, and resumability.

## Current State Analysis

### Strengths
- ✅ Good separation of concerns with controller/model/util structure
- ✅ SQLite-based persistence with GraphDbImpl
- ✅ Async HTTP requests with rate limiting
- ✅ Iterative crawling with frontier management
- ✅ Relationship type resolution with lazy evaluation
- ✅ Comprehensive test coverage structure
- ✅ Modern Python tooling (ruff, mypy, pytest)

### Current Limitations
- ❌ In-memory SQLite database doesn't scale for large datasets
- ❌ No graceful shutdown/resume capability
- ❌ Fixed rate limiting without adaptive throttling
- ❌ No progress monitoring or health checks
- ❌ Limited error recovery and retry mechanisms
- ❌ No distributed crawling capability
- ❌ Memory usage grows unbounded during large crawls

## Refactoring Goals

1. **Scalability**: Handle millions of individuals and relationships
2. **Persistence**: Robust state management with crash recovery
3. **Rate Limiting**: Respectful, adaptive API usage
4. **Resumability**: Suspend/resume crawls at any point
5. **Monitoring**: Progress tracking and health metrics
6. **Reliability**: Enhanced error handling and recovery
7. **Performance**: Optimized memory usage and I/O

## Implementation Plan

### Phase 1: Core Infrastructure Improvements

#### 1.1 Enhanced Database Layer
**Priority**: High | **Effort**: Medium | **Risk**: Low

- **Objective**: Replace in-memory SQLite with persistent, scalable storage
- **Tasks**:
  - Migrate from in-memory to file-based SQLite with WAL mode
  - Add database connection pooling
  - Implement proper transaction management
  - Add database schema versioning/migrations
  - Create database backup/restore functionality
- **Files to modify**: `fscrawler/model/graph_db_impl.py`
- **New files**: `fscrawler/model/database.py`, `fscrawler/migrations/`

#### 1.2 State Management System
**Priority**: High | **Effort**: Medium | **Risk**: Medium

- **Objective**: Implement comprehensive crawl state persistence
- **Tasks**:
  - Create CrawlState class to track crawl progress
  - Implement checkpoint system for regular state saves
  - Add crawl metadata (start time, parameters, statistics)
  - Create state validation and recovery mechanisms
- **Files to modify**: `fscrawler/crawler.py`
- **New files**: `fscrawler/model/crawl_state.py`, `fscrawler/model/checkpoint.py`

#### 1.3 Configuration Management
**Priority**: Medium | **Effort**: Low | **Risk**: Low

- **Objective**: Centralized, flexible configuration system
- **Tasks**:
  - Create configuration schema with validation
  - Support for environment variables and config files
  - Rate limiting configuration
  - Database configuration options
- **New files**: `fscrawler/config.py`, `config.yaml`

### Phase 2: Rate Limiting and API Management

#### 2.1 Adaptive Rate Limiting
**Priority**: High | **Effort**: Medium | **Risk**: Medium

- **Objective**: Implement intelligent, respectful API usage
- **Tasks**:
  - Create rate limiter with exponential backoff
  - Implement 429 response handling
  - Add circuit breaker pattern for API failures
  - Monitor API response times and adjust rates
  - Add rate limiting metrics and logging
- **Files to modify**: `fscrawler/controller/session.py`, `fscrawler/controller/fsapi.py`
- **New files**: `fscrawler/controller/rate_limiter.py`

#### 2.2 Request Queue Management
**Priority**: Medium | **Effort**: Medium | **Risk**: Medium

- **Objective**: Better request batching and prioritization
- **Tasks**:
  - Implement priority queue for different request types
  - Add request deduplication
  - Batch optimization for person requests
  - Request retry mechanisms with jitter
- **Files to modify**: `fscrawler/controller/fsapi.py`
- **New files**: `fscrawler/controller/request_queue.py`

### Phase 3: Crawl Management and Control

#### 3.1 Crawl Controller Refactoring
**Priority**: High | **Effort**: High | **Risk**: Medium

- **Objective**: Robust crawl orchestration with suspend/resume
- **Tasks**:
  - Refactor main crawl loop for better control
  - Implement graceful shutdown handling (SIGTERM, SIGINT)
  - Add crawl pause/resume functionality
  - Create crawl progress tracking
  - Add crawl statistics and reporting
- **Files to modify**: `fscrawler/crawler.py`
- **New files**: `fscrawler/controller/crawl_manager.py`

#### 3.2 Signal Handling and Graceful Shutdown
**Priority**: High | **Effort**: Low | **Risk**: Low

- **Objective**: Clean shutdown without data loss
- **Tasks**:
  - Implement signal handlers for SIGTERM/SIGINT
  - Ensure current batch completion before shutdown
  - Save state on shutdown
  - Add shutdown timeout handling
- **Files to modify**: `fscrawler/crawler.py`

#### 3.3 Progress Monitoring and Metrics
**Priority**: Medium | **Effort**: Medium | **Risk**: Low

- **Objective**: Comprehensive crawl monitoring
- **Tasks**:
  - Create metrics collection system
  - Add progress bars and ETA calculations
  - Implement health checks
  - Add performance metrics (requests/sec, memory usage)
  - Create status reporting endpoints
- **New files**: `fscrawler/monitoring/metrics.py`, `fscrawler/monitoring/health.py`

### Phase 4: Error Handling and Resilience

#### 4.1 Enhanced Error Recovery
**Priority**: High | **Effort**: Medium | **Risk**: Low

- **Objective**: Robust error handling and recovery
- **Tasks**:
  - Implement comprehensive exception handling
  - Add retry mechanisms with exponential backoff
  - Create error classification system
  - Add dead letter queue for failed requests
  - Implement partial failure recovery
- **Files to modify**: `fscrawler/controller/fsapi.py`, `fscrawler/controller/session.py`
- **New files**: `fscrawler/controller/error_handler.py`

#### 4.2 Data Validation and Integrity
**Priority**: Medium | **Effort**: Medium | **Risk**: Low

- **Objective**: Ensure data quality and consistency
- **Tasks**:
  - Add data validation for API responses
  - Implement consistency checks
  - Create data repair mechanisms
  - Add duplicate detection and handling
- **Files to modify**: `fscrawler/model/individual.py`
- **New files**: `fscrawler/validation/data_validator.py`

### Phase 5: Performance and Scalability

#### 5.1 Memory Management
**Priority**: High | **Effort**: Medium | **Risk**: Medium

- **Objective**: Optimize memory usage for large crawls
- **Tasks**:
  - Implement streaming data processing
  - Add memory usage monitoring
  - Create data pagination for large result sets
  - Optimize database queries
  - Add memory pressure handling
- **Files to modify**: `fscrawler/model/graph_db_impl.py`

#### 5.2 Database Optimization
**Priority**: Medium | **Effort**: Medium | **Risk**: Low

- **Objective**: Optimize database performance
- **Tasks**:
  - Add database indexing strategy
  - Implement query optimization
  - Add database maintenance routines
  - Create database partitioning for large datasets
- **Files to modify**: `fscrawler/model/graph_db_impl.py`

#### 5.3 Concurrent Processing
**Priority**: Medium | **Effort**: High | **Risk**: High

- **Objective**: Improve processing throughput
- **Tasks**:
  - Optimize async request handling
  - Implement worker pool pattern
  - Add request batching optimization
  - Create processing pipeline
- **Files to modify**: `fscrawler/controller/fsapi.py`

### Phase 6: Advanced Features

#### 6.1 Crawl Strategies
**Priority**: Low | **Effort**: High | **Risk**: Medium

- **Objective**: Flexible crawling strategies
- **Tasks**:
  - Implement breadth-first vs depth-first options
  - Add targeted crawling (specific families/regions)
  - Create smart frontier management
  - Add crawl boundary conditions
- **New files**: `fscrawler/strategies/`

#### 6.2 Data Export and Integration
**Priority**: Low | **Effort**: Medium | **Risk**: Low

- **Objective**: Enhanced data export capabilities
- **Tasks**:
  - Add multiple export formats (JSON, CSV, GraphML)
  - Create incremental export functionality
  - Add data streaming export
  - Implement export validation
- **Files to modify**: `fscrawler/controller/graph_writer.py`

### Phase 7: Testing and Quality Assurance

#### 7.1 Enhanced Test Suite
**Priority**: Medium | **Effort**: High | **Risk**: Low

- **Objective**: Comprehensive test coverage
- **Tasks**:
  - Add integration tests for new components
  - Create performance benchmarks
  - Add chaos engineering tests
  - Implement test data generators
  - Add end-to-end crawl tests
- **Files to modify**: `tests/`

#### 7.2 Code Quality Improvements
**Priority**: Medium | **Effort**: Medium | **Risk**: Low

- **Objective**: Maintain high code quality
- **Tasks**:
  - Add comprehensive type hints
  - Improve documentation and docstrings
  - Add code complexity analysis
  - Create architectural decision records
- **Files to modify**: All Python files

## Implementation Timeline

### Sprint 1 (2 weeks): Foundation
- [ ] Enhanced Database Layer (1.1)
- [ ] Configuration Management (1.3)
- [ ] Basic State Management (1.2)

### Sprint 2 (2 weeks): Rate Limiting
- [ ] Adaptive Rate Limiting (2.1)
- [ ] Signal Handling (3.2)
- [ ] Enhanced Error Recovery (4.1)

### Sprint 3 (2 weeks): Crawl Control
- [ ] Crawl Controller Refactoring (3.1)
- [ ] Progress Monitoring (3.3)
- [ ] Request Queue Management (2.2)

### Sprint 4 (2 weeks): Optimization
- [ ] Memory Management (5.1)
- [ ] Database Optimization (5.2)
- [ ] Data Validation (4.2)

### Sprint 5 (1 week): Testing and Polish
- [ ] Enhanced Test Suite (7.1)
- [ ] Code Quality Improvements (7.2)
- [ ] Documentation updates

## Risk Assessment

### High Risk Items
- **Concurrent Processing (5.3)**: Complex async patterns, potential race conditions
- **State Management (1.2)**: Critical for resumability, complex state transitions

### Medium Risk Items
- **Adaptive Rate Limiting (2.1)**: Must balance speed vs. API respect
- **Crawl Controller Refactoring (3.1)**: Core logic changes, potential regressions
- **Memory Management (5.1)**: Performance critical, potential memory leaks

### Mitigation Strategies
- Incremental implementation with feature flags
- Comprehensive testing at each phase
- Rollback plans for each major change
- Performance monitoring throughout

## Success Metrics

### Scalability
- [ ] Handle 1M+ individuals without memory issues
- [ ] Process 10K+ relationships per hour
- [ ] Support crawls running for weeks/months

### Reliability
- [ ] 99.9% uptime during long crawls
- [ ] Zero data loss on unexpected shutdowns
- [ ] Automatic recovery from API failures

### Performance
- [ ] <10MB memory per 1K individuals
- [ ] <1 second average API response time
- [ ] 95% reduction in duplicate API calls

### Usability
- [ ] One-command suspend/resume
- [ ] Real-time progress monitoring
- [ ] Clear error messages and recovery guidance

## Code Quality Standards

### General Principles
- Follow existing code style (ruff, mypy)
- Maintain backward compatibility where possible
- Add comprehensive logging
- Include type hints for all new code
- Write docstrings for all public methods

### Testing Requirements
- Unit tests for all new functionality
- Integration tests for API interactions
- Performance tests for scalability features
- Mock external dependencies appropriately

### Documentation Requirements
- Update README with new features
- Add configuration documentation
- Create troubleshooting guide
- Document API rate limiting best practices

## Conclusion

This refactoring plan transforms the fs-crawler from a small-scale tool into an enterprise-grade genealogy data extraction system. The phased approach ensures manageable implementation while maintaining system stability. The focus on persistence, rate limiting, and resumability addresses the core requirements for large-scale FamilySearch crawling.

The plan prioritizes the most critical improvements first (database persistence, rate limiting, state management) while building toward advanced features. Each phase delivers tangible value and can be deployed independently, reducing implementation risk.

Success will be measured by the system's ability to handle million-record crawls over extended periods while maintaining data integrity and respecting FamilySearch's API limits.
