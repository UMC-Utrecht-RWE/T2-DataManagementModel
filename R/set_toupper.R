# Author: RWE - UMC Utrecht
# email: Y.Mao@umcutrecht.nl
# Organisation: UMC Utrecht, Utrecht, The Netherlands
# Date: 20/02/2024

# set selected columns (vars) to upper case
# -- dependent package: tidyverse

set_toupper <- function(dat, vars){
  dat %>% 
    mutate(across(all_of(vars),toupper))
}