test_that("file import runs successfully", {
  vx <- import_file("dbtest/VACCINES.csv")

  expect_equal(class(vx), c("data.table", "data.frame"))
  expect_equal(dim(vx), c(20, 12))

  # all columns must be character!
  expect_true(all(sapply(vx, class) == "character"))
})
