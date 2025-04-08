test_that("file import runs successfully", {
  vx <- import_file("dbtest/VACCINES.csv")

  expect_equal(class(vx), c("data.table", "data.frame"))
  expect_equal(dim(vx), c(20, 12))

  # all columns must be character!
  expect_true(all(sapply(vx, class) == "character"))
})

test_that("import foreign characters", {
  # the file to import is Latin-1 encoded
  expect_error(import_file("dbtest/dap_specific_concept_map_unspecified.csv"),
               regexp = 'input string 15 is invalid UTF-8')             
  
  # the file to import is utf-8 encoded
  vx <- import_file("dbtest/dap_specific_concept_map_utf8.csv")
  expect_equal(dim(vx), c(2, 12))
})
