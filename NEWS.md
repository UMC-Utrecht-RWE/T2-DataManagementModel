# T2-DataManagementModel 1.0.0

## New features

- Added `apply_codelist()` to apply harmonized codelists to a CDM database connection.
  - Includes input validation and required-column checks.
  - Supports grouping codelist rows into families.
  - Handles parent/child code processing through SQL-backed steps.
  - Uses new SQL templates for concept creation and cleanup.
- Added `initialize_concept_table()` to create the target `concept_table`.
- Added `wrangling_codelist()` to reshape a prepared codelist into the long format required by `apply_codelist()`.
- Added `wrangling_concept_map()` to reshape DAP-specific concept maps into the long format required by `apply_codelist()`.
- Added `get_sql()` to read SQL scripts from file and prepare them for execution.

## Changed

- Refactored the package around a new codelist application workflow using DuckDB and SQL templates.
- Updated `DatabaseLoader` to use YAML configuration instead of JSON.
- Updated package dependencies, namespace imports, and documentation to match the new workflow.
- Reworked several internal helpers to use the new `scheme` naming and database layout.
- Added pkgdown documentation and a new vignette for the concept management workflow.

## Removed

- Removed `append_tables()`.
- Removed `create_indexes()`.
- Removed legacy script-loading helpers:
  - `R/scripts/load_function.R`
  - `R/scripts/load_package.R`
- Removed `R/scripts/CDM_metadata.rds` from the package source.
- Removed the old `Workflow` section from `README.md`.

## Deprecated

- No explicit deprecation framework was introduced in the PR.
- However, the following older workflow appears to be superseded by the new codelist pipeline and should be considered legacy:
  - direct use of the old script-based loading helpers
  - older ad hoc table-appending/index-creation utilities
  - JSON-based loader configuration
