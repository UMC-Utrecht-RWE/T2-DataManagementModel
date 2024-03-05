#' Import program
#' 
#' Import meta data file based on directory and file name specified in the program_list
#'
#' @param program_list program list
#' @param fl file name
#'
#' @return
#' @export
import_program <- function(program_list, fl){
  program1 <- program_list |> dplyr::filter(file_name== fl) # TODO remove dplyr dependency
  out <- import_file(file.path(get(program1$folder_variable),program1$file))
  return(out)
}
