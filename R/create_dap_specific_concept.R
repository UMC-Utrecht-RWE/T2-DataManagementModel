#' Create DAP-Specific Concepts into Database
#'
#' This function Creates DAP-specific concepts into a database using a
#' DAP-specifc concept map (codelist).
#' The function modifies the specified save_db by creating edited tables
#' and Creating DAP-specific concepts.
#'
#' @param codelist A data.table containing information about tables,
#' columns, and values for DAP-specific concepts.
#' @param name_attachment Name of the table to be insert into the database.
#' @param save_db The database connection object where the edited tables
#' and concepts will be saved.
#' @param date_col_filter An optional filter to subset data based on
#' a specified date column.
#' @param dir_save Directory path for parquet output (required if save_in_parquet = TRUE)
#' @param table_name Name of the table in the codelist database.
#' Default: "cdm_table_name".
#' @param column_name_prefix An optional string that defines the prefix name
#' of the column name variable column(s) from the DAP-specific concept map.
#' Default: "column_name".
#' @param expected_value_prefix An optional string that defines the prefix name
#' of the expected value variable column(s) from the DAP-specific concept map.
#' Default: "expected_value".
#' @param add_meaning A boolean that defines whether to save the meaning of any
#' CDM table -if available- in the results of the function.
#' This is specific for the ConcePTION CDM. Default: FALSE.]
#' @param save_in_parquet Logical; if TRUE, exports to parquet;
#' if FALSE, inserts to concept_table
#' @param intermediate_type Type of intermediate structure to create.
#' @param keep_date_prefix Characther default: keep_date.
#' The prefix value to identify the column where the column name with the
#' date will be stored in the concept_table.
#' @param keep_column_prefix Characther default: keep_value
#' The prefix value to identify the column where the column name with the value
#' will stored in the concept_table.

#' @param partition_var Default: concept_id.
#' Concept_table column to partition on.

#' @export
create_dap_specific_concept <- function(
  codelist,
  name_attachment,
  save_db,
  date_col_filter = NULL,
  dir_save = NULL,
  add_meaning = FALSE,
  save_in_parquet = FALSE,
  table_name = "cdm_table_name",
  column_name_prefix = "column_name",
  expected_value_prefix = "expected_value",
  keep_date_prefix = "keep_date",
  keep_column_prefix = "keep_value",
  intermediate_type = "TABLE",
  partition_var = "concept_id"
) {
  if (nrow(codelist) <= 0) {
    stop("Codelist does not contain any data.")
  }
  if (any(intermediate_type == c("TABLE", "VIEW")) != TRUE) {
    stop("intermediate_type has to be either TABLE or VIEW.")
  }
  # Adding . to attachement name in case it is missing it.
  # (Useuful for query later)
  name_attachment <- base::ifelse(base::endsWith(name_attachment, "."),
    name_attachment, base::paste0(name_attachment, ".")
  )

  scheme <- unique(codelist[[table_name]])

  cols_names <- base::grep(
    pattern = base::paste0("^", column_name_prefix),
    x = base::names(codelist),
    value = TRUE
  )
  cols <- codelist[, cols_names, with = FALSE]

  value_names <- base::grep(
    pattern = base::paste0("^", expected_value_prefix),
    x = base::names(codelist),
    value = TRUE
  )
  values <- codelist[, value_names, with = FALSE]

  keep_date_names <- base::grep(
    pattern = base::paste0("^", keep_date_prefix),
    x = base::names(codelist),
    value = TRUE
  )

  keep_value_names <- base::grep(
    pattern = base::paste0("^", keep_column_prefix),
    x = base::names(codelist),
    value = TRUE
  )
  ## PHASE 1
  # For each unique table name in codelist:
  for (name in scheme) {
    name_edited <- base::paste0(name, "_EDITED")
    # collects columns that should be uppercased for matching (to_upper_cols)
    to_upper_cols <- base::unique(
      stats::na.omit(
        unlist(codelist[get(table_name) %in% name, cols_names, with = FALSE])
      )
    )
    # collects date/value passthrough columns (keep_date, keep_value)
    keep_date <- base::unique(
      stats::na.omit(
        unlist(
          codelist[get(table_name) %in% name, keep_date_names, with = FALSE]
        )
      )
    )
    keep_value <- base::unique(
      stats::na.omit(
        unlist(
          codelist[get(table_name) %in% name, keep_value_names, with = FALSE]
        )
      )
    )
    # removes duplicates where keep_value overlaps uppercased columns
    keep_value <- keep_value[!keep_value %in% to_upper_cols]
    # builds SQL select fragments:
    to_upper_query <- base::paste0(
      base::paste0(
        " UPPER(", to_upper_cols, ") AS ", to_upper_cols
      ),
      collapse = ", "
    )
    dates_query <- base::paste0(
      base::paste0(
        " TRY_CAST(", keep_date, " AS DATE) AS ", keep_date
      ),
      collapse = ", "
    )

    select_cols_query <- base::paste0(keep_value, collapse = ", ")

    if (!name_edited %in% DBI::dbListTables(save_db) ||
      all(
        c(keep_value, dates_query, to_upper_cols) %in%
          DBI::dbListFields(save_db, name_edited)
      ) == FALSE) {
      # optionally finds a meaning column if add_meaning=TRUE
      meaning_column_name <- ""
      if (add_meaning) {
        columns_db_table <- DBI::dbGetQuery(
          save_db,
          base::paste0("PRAGMA table_info('", name_attachment, name, "')")
        )$name
        meaning_column_name <- columns_db_table[
          stringr::str_detect(columns_db_table, "meaning")
        ]
        # Checking if we already retrieve meaning through the codelist
        if (length(meaning_column_name) > 0 &&
              (any(meaning_column_name %in% to_upper_cols) ||
                 any(meaning_column_name %in% keep_value))) {
          meaning_column_name <- ""
        }
        base::print(base::paste0(
          "[create_dap_specific_concept] Meaning not identified for: ",
          name_edited
        ))
        meaning_column_name <- ""
      }

      # creates temp object ${table}_EDITED_dapspec with
      # ori_table, unique_id, person_id and
      # selected passthrough columns, uppercase-normalized match columns,
      # optional meaning, casted dates.
      DBI::dbExecute(
        save_db,
        base::paste0(
          "CREATE TEMP ",
          intermediate_type, " ", name_edited, "_dapspec AS
          SELECT ori_table, unique_id, person_id, ",
          if (base::nchar(select_cols_query) > 0) {
            base::paste0(select_cols_query, ",")
          },
          if (base::nchar(to_upper_query) > 0) {
            base::paste0(to_upper_query, ",")
          },
          if (base::nchar(meaning_column_name) > 0) {
            base::paste0(meaning_column_name, ",")
          },
          dates_query, " FROM ", name_attachment, name
        )
      )
    }
  }

  ## PHASE 2
  # For each row in codelist:
  for (num in seq_len(nrow(codelist))) {
    table_temp <- codelist[[num, table_name]]
    name_edited <- base::paste0(table_temp, "_EDITED_dapspec")
    concept_name <- codelist[[num, "concept_id"]]
    date_col <- codelist[[num, "keep_date_column_name"]]
    codelist_id <- codelist[[num, "dap_spec_id"]]

    # gets row-specific match columns + expected values, uppercases
    # expected values, removes "NA" strings
    cols_temp <- base::unique(stats::na.omit(base::unlist(cols[num, ])))
    values_temp <- base::toupper(
      base::unique(stats::na.omit(base::unlist(values[num, ])))
    )
    cols_temp <- base::setdiff(cols_temp, "NA")
    values_temp <- base::setdiff(values_temp, "NA")

    value <- codelist[[num, "keep_value_column_name"]]
    if (add_meaning) {
      columns_db_table <- DBI::dbListFields(save_db, name_edited)
      meaning_column_name <- columns_db_table[
        stringr::str_detect(columns_db_table, "meaning")
      ]
      if (base::length(meaning_column_name) > 0) {
        meaning_clause <- base::paste0(
          ", ", meaning_column_name,
          " AS meaning "
        )
      } else {
        base::print(base::paste0(
          "[create_dap_specific_concept] Meaning not identified for: ",
          name_edited
        ))
        meaning_clause <- base::paste0(", NULL AS meaning ")
      }
    } else {
      meaning_clause <- ""
    }

    if (base::is.null(value)) {
      value <- TRUE
    } else if (any(is.na(value))) {
      value <- TRUE
    }
    if (base::is.null(date_col)) {
      date_col <- "NULL"
    } else if (any(is.na(date_col))) {
      date_col <- "NULL"
    }

    coding_system <- base::paste0("'", codelist_id, "'")
    where_statement <- base::paste(
      base::paste(
        cols_temp,
        base::paste0(
          "'", values_temp, "'"
        ),
        sep = " = "
      ),
      collapse = " AND "
    )
    if (!is.null(date_col_filter) && date_col != "NULL") {
      where_statement <- base::paste0(
        where_statement, " AND ",
        date_col, " >= DATE '", date_col_filter, "'"
      )
    }
  
    print(where_statement)
    # Execute COPY to parquet OR INSERT to database table
    if (save_in_parquet) {
      if (!is.null(partition_var)) {
        # Export filtered data to parquet format, partitioned by partition_var
        DBI::dbExecute(
          save_db,
          base::paste0(
            "COPY ( SELECT t1.ori_table, t1.unique_id, t1.person_id, ",
            coding_system, " AS code, ",
            coding_system, " AS coding_system, ",
            value, " AS value, '",
            concept_name, "' AS concept_id, ",
            date_col, " AS date ",
            meaning_clause, ", 1 AS tag FROM ",
            name_edited, " t1",
            " WHERE ", where_statement, ") TO '", dir_save,
            "'(FORMAT PARQUET, PARTITION_BY (",
            partition_var, "), APPEND TRUE);"
          )
        )
      } else {
        # Export filtered data to parquet format, partitioned by concept_id
        DBI::dbExecute(
          save_db,
          base::paste0(
            "COPY ( SELECT t1.ori_table, t1.unique_id, t1.person_id, ",
            coding_system, " AS code, ",
            coding_system, " AS coding_system, ",
            value, " AS value, '",
            concept_name, "' AS concept_id, ",
            date_col, " AS date ",
            meaning_clause, ", 1 AS tag FROM ",
            name_edited, " t1  WHERE ", where_statement,
            ") TO '", dir_save, "'(FORMAT PARQUET, APPEND TRUE);"
          )
        )
      }
    } else {
      # Insert filtered data into concept_table in the database
      rs <- DBI::dbSendStatement(
        save_db,
        base::paste0(
          "INSERT INTO concept_table
          SELECT t1.ori_table, t1.unique_id, t1.person_id, ",
          coding_system, " AS code, ",
          coding_system, " AS coding_system, ",
          value, " AS value, '",
          concept_name, "' AS concept_id, ",
          date_col, " AS date ",
          meaning_clause, "FROM ",
          name_edited, " t1 WHERE ", where_statement
        )
      )
      # Clear the result set
      DBI::dbClearResult(rs)
    }
  }
}
