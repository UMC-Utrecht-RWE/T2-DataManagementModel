test_that("check append tables", {
  ref_filepath <- system.file("extdata",
                              "combined_codelist_drugs_vaccines_algo_additional.csv",
                              package = "metadatachecker")
  ref_colname <- "codenames"
  meta_filpath <- system.file("extdata",
                              "Pfizer_study_variables.csv",
                              package = "metadatachecker")
  meta_colname <- "VarName"
  results <- check_study_variables(ref_filepath,
                                   ref_colname,
                                   meta_filpath,
                                   meta_colname)
  expect_equal(results, c("L_SEX_COV","O_YEARBIRTH_COV","L_RACE_COV"))
})
