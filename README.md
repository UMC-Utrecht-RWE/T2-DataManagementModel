
# RWE-DataManagementModel

<!-- badges: start -->
[![R-CMD-check](https://github.com/UMC-Utrecht-RWE/T2-DataManagementModel/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/UMC-Utrecht-RWE/T2-DataManagementModel/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

RWE-DataManagementModel is an R package that provides a data model and functionality for processing RWD.

## Installation

You can install the current version of RWE-DataManagementModel as follows:

1. In your R console, ensure you have the package `devtools` installed:
   ```
   install.packages("devtools")
   ```
1. Using devtools, install the package directly from GitHub
   ```
   devtools::install_github('UMC-Utrecht-RWE/T2-DataManagementModel', dependencies = TRUE)
   ```

### Dependency `data.table` on mac

While the `data.table` library should install automatically upon installation of the package, it may not function optimally through this installation process when installed on a mac (which will be the case for most users of this package).
It is recommended to take a look at the [Mac-specific installation instructions](https://github.com/Rdatatable/data.table/wiki/Installation#Enable-openmp-for-macos) for `data.table` and OpenMP support, as well as [this issue](https://github.com/Rdatatable/data.table/issues/5419) with a detailed workaround for M1/2 chips.

You can verify `data.table` functionality with:
```
data.table::test.data.table()
```

## Workflow

![Overview data management](T2_DMM.png)

