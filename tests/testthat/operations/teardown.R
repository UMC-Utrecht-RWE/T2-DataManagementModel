cat("Running global teardown...\n")
file.remove(Sys.getenv("SHARED_METADATA_PATH"))
file.remove(Sys.getenv("CONFIG_PATH"))

db_path <- Sys.getenv("SYNTHETIC_DB_PATH", unset = NA)
if (!is.na(db_path) && file.exists(db_path)) {
  file.remove(db_path)
}

Sys.unsetenv("SYNTHETIC_DB_PATH")
