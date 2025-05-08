cat("Running global teardown...\n")
file.remove(Sys.getenv("SHARED_METADATA_PATH"))
file.remove(Sys.getenv("CONFIG_PATH"))
