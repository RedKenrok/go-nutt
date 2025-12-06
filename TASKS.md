# Tasks

## 1. Input validation and sanitization

- Current state: Basic checks exist (e.g., empty keys), but not comprehensive.
- Improvements:
  - Validate struct tags and field types more rigorously during NewStore (e.g., ensure index fields are strings or
 comparable).
  - Sanitize inputs like bucket names or keys to prevent injection or invalid characters.
  - Add length limits for keys and values to prevent abuse.

## 2. Testing and coverage

- Current state: Good test coverage for basic operations.
- Improvements:
  - Add fuzz tests for query conditions and edge cases (e.g., malformed data).
  - Test with larger datasets and concurrent loads to simulate real usage.
  - Add integration tests for WAL recovery and crash scenarios.
  - Include benchmarks for memory usage and CPU.

## 3. Performance optimizations

- Current state: Uses pools for buffers and decoders, which is good.
- Improvements:
  - Profile the code to identify bottlenecks (e.g., reflection in queries might be slow for large datasets).
  - Optimize query intersection logic (e.g., use more efficient set operations).
  - Implement lazy loading or pagination for large result sets.

## 4. Concurrency and thread safety

- Current state: Uses mutexes for WAL and buffers.
- Improvements:
  - Audit for potential race conditions (e.g., in Flush or index updates).
  - Add context support (context.Context) to operations for cancellation and timeouts.
  - Consider using channels or goroutine pools for background tasks like flushing.

## 5. Security and Reliability

- Current State: Basic file I/O with permissions.
- Improvements:
  - Add checksums to WAL entries for corruption detection.
  - Harden against file system failures (e.g., disk full).

## 6. Code organization and maintainability

- Current state: Well-structured into packages.
- Improvements:
  - Extract interfaces (e.g., Storer interface) for testability and extensibility.
  - Refactor large functions (e.g., getCandidateKeysTx) into smaller, testable units.
  - Add more documentation (e.g., godoc examples.)

## 7. Expand functionality

- Current state: Query functionality is only used for getting data.
- Improvements:
  - Rename `Query` into `GetQuery`.
  - Rename `QueryCount` into `CountQuery`.
  - Add `Count` function which list length of provided index.
  - Add `DeleteQuery` function which reuses code of query function.
  - Update documentation (`README.md`.)
