# Author: RWE - UMC Utrecht
# email: Y.Mao@umcutrecht.nl
# Organisation: UMC Utrecht, Utrecht, The Netherlands
# Date: 20/02/2024 #nolint

# Load of functions from folder "source_code/functions"

all_files <- list.files(
  path = transformations_T2_semantic_harmonization_source_code_functions,
  full.names = TRUE, recursive = TRUE
)
for (fl in all_files) {
  source(fl)
}
