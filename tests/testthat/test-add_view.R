library(DBI)
library(duckdb)
library(testthat)

# Connect to an in-memory DuckDB
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

# Create a sample table
dbExecute(con, "
CREATE TABLE persons (
  name TEXT,
  age INTEGER
);
")

dbExecute(con, "
INSERT INTO persons VALUES
('Alice', 25),
('Bob', 30),
('Alice', 25),
('Charlie', 17)
;
")

# Ensure the registry table exists
dbExecute(con, "
CREATE TABLE IF NOT EXISTS _pipeline_registry (
  pipeline_name TEXT PRIMARY KEY,
  current_view TEXT
)
")

# ---- TESTS ----
test_that("Pipeline auto-initializes if it does not exist", {
  add_view(con, pipeline = "test_pipeline", "SELECT * FROM %s", base_table = "persons", final_alias = "test_view")
  
  # Check registry
  registry <- dbGetQuery(con, "SELECT * FROM _pipeline_registry WHERE pipeline_name='test_pipeline'")
  expect_equal(nrow(registry), 1)
  expect_equal(registry$current_view, "test_pipeline_v2")
  
  # Check final alias exists
  views <- dbGetQuery(con, "SELECT table_name FROM information_schema.views WHERE table_name='test_view'")
  expect_equal(nrow(views), 1)
})

test_that("Adding multiple steps creates versioned views and updates final alias", {
  # Step 2: remove duplicates
  add_view(con, "test_pipeline", "SELECT DISTINCT * FROM %s", final_alias = "test_view")
  
  # Step 3: add computed column
  add_view(con, "test_pipeline", "SELECT *, LENGTH(name) AS name_len FROM %s", final_alias = "test_view")
  
  # Check registry points to latest version
  registry <- dbGetQuery(con, "SELECT current_view FROM _pipeline_registry WHERE pipeline_name='test_pipeline'")
  expect_equal(registry$current_view, "test_pipeline_v4")
  
  # Final alias points to latest version
  sql <- dbGetQuery(con, "SELECT view_definition FROM information_schema.views WHERE table_name='test_view'")$view_definition
  expect_true(grepl("test_pipeline_v4", sql))
})

test_that("Final view returns expected data", {
  result <- dbGetQuery(con, "SELECT * FROM test_view ORDER BY name")
  
  # Check that duplicates are removed
  expect_equal(nrow(result), 3)
  
  # Check that name_len column exists
  expect_true("name_len" %in% colnames(result))
  
  # Check that values are correct
  expect_equal(result$name_len[ result$name == "Alice"], nchar("Alice"))
  expect_equal(result$name_len[ result$name == "Bob"], nchar("Bob"))
})

# Cleanup
dbDisconnect(con, shutdown = TRUE)
