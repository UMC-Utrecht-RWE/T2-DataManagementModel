# run_checks.R

# Load required packages
if (!requireNamespace("lintr", quietly = TRUE)) install.packages("lintr")
if (!requireNamespace("styler", quietly = TRUE)) install.packages("styler")
if (!requireNamespace("roxygen2", quietly = TRUE)) install.packages("roxygen2")

# Run `lintr` to check code style
cat("\nRunning lintr (style checks)...\n")
lint_results <- lintr::lint_package()
print(lint_results)

# Run `styler` to check for formatting issues
cat("\nRunning styler (code formatting checks)...\n")
styler::style_pkg(dry = "off")  # Dry run to check if reformatting is needed

# Run `roxygen2` to check if documentation is properly generated
cat("\nRunning roxygen2 (documentation checks)...\n")
roxygen2::roxygenise()

cat("\nAll checks completed!")
