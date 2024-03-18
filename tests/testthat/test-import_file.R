test_that("file import runs successfully", {
  vx <- import_file("dbtest/VACCINES.csv")
  
  expect_equal(class(vx), c("data.table", "data.frame"))
  expect_equal(dim(vx), c(20, 12))
})
