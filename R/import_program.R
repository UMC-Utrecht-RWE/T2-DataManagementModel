# Author: RWE - UMC Utrecht
# email: Y.Mao@umcutrecht.nl
# Organisation: UMC Utrecht, Utrecht, The Netherlands
# Date: 21/02/2024

# import meta data file based on directory and file name specified in the program_list
import_program <- function(program_list, fl){
  program1 <- program_list %>% filter(file_name== fl)
  out <- import_file(file.path(get(program1$folder_variable),program1$file))
  out
}
