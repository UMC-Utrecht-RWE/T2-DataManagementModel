
# T2 - Data Management Model

<!-- badges: start -->
[![R-CMD-check](https://github.com/UMC-Utrecht-RWE/T2-DataManagementModel/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/UMC-Utrecht-RWE/T2-DataManagementModel/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

## Documentation

The full documentation and vignettes are available at:

👉 https://UMC-Utrecht-RWE.github.io/T2-DataManagementModel/

For example, see the concept management model vignette:
👉 https://UMC-Utrecht-RWE.github.io/T2-DataManagementModel/articles/concept_management_model.html

## Installation

You can install the current version of T2.DMM as follows:

1. In your R console, ensure you have the package `devtools` installed:
   ```
   install.packages("devtools")
   ```
1. Using devtools, install the package directly from GitHub
   ```
   devtools::install_github('UMC-Utrecht-RWE/T2-DataManagementModel', dependencies = TRUE)
   ```

### Dependency `data.table` on mac

While the `data.table` library should install automatically upon package installation, it may not function optimally when installed through this automatic process on a Mac (which will be the case for most users of this package).
It is recommended to take a look at the [Mac-specific installation instructions](https://github.com/Rdatatable/data.table/wiki/Installation#Enable-openmp-for-macos) for `data.table` and OpenMP support, as well as [this issue](https://github.com/Rdatatable/data.table/issues/5419) with a detailed workaround for M1/2 chips.

You can verify `data.table` functionality with:
```
data.table::test.data.table()
```
