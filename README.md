
# RWE-DataManagementModel

<!-- badges: start -->
<!-- badges: end -->

RWE-DataManagementModel is an R package that provides a data model and functionality for processing RWD.

![Overview data management](T2_DMM.png)

## Installation

You can install the development version of RWE-DataManagementModel (or the latest stable release) like so:

Using devtools (recommended):

Download and unzip (in your working enviroment) or clone the repo. Let's say your folder is here:

- on Mac, the *"path_to_the_directory_of_the_package"* looks something like this: "/Users/Name/Desktop/RWE-DataManagementModel"
- on Windows, the *"path_to_the_directory_of_the_package"* looks something like this: "C:/Users/Name/Desktop/RWE-DataManagementModel" (it should be **"/"**, and not "\\")

and on R console, type the following:
``` r
# If you don't have devtools, install it
install.packages("devtools")
#Install RWE-DataManagementModel  by typing this on R console
devtools::install("path_to_the_directory_of_the_package", dependencies = TRUE)
```
If it asks about updating the packages that are available in your system, I usually skip it and hope that I won't break anything. So far it worked, but please check it.


## Example

Here is how you can use metadatachecker: 


On R console, type the following:
``` r
library(RWE-DataManagementModel)
```
