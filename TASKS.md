# Tasks

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
