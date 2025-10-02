decode_qvd_reference <- function(qvd,
                                 prefer_numeric_duals = TRUE,
                                 temporal_columns = integer()) {
  meta <- qvd[["metadata"]]
  if (is.null(meta)) {
    stop("read_qvd result does not include metadata; call with include_indices = TRUE")
  }

  header <- meta$table_header
  fields <- header$fields
  n_fields <- length(fields)
  n_rows <- header$no_of_records
  indices <- meta$indices

  numeric_cols <- vector("list", n_fields)
  text_cols <- vector("list", n_fields)

  double_to_string <- function(value) {
    if (is.na(value)) {
      return(NA_character_)
    }
    out <- sprintf("%.15f", value)
    out <- sub("0+$", "", out)
    out <- sub("\\.$", "", out)
    if (identical(out, "")) {
      "0"
    } else {
      out
    }
  }

  int_to_string <- function(value) {
    if (is.na(value)) {
      return(NA_character_)
    }
    sprintf("%d", value)
  }

  for (f in seq_len(n_fields)) {
    numeric_cols[[f]] <- rep(NA_real_, n_rows)
    text_cols[[f]] <- rep(NA_character_, n_rows)
  }

  assign_symbol <- function(row, f, symbol_row) {
    type <- symbol_row$type
    string_val <- symbol_row$string
    int_val <- symbol_row$int
    double_val <- symbol_row$double

    has_string <- !is.na(string_val) && nzchar(string_val)

    if (type %in% c(0x01, 0x05)) {
      numeric_cols[[f]][row] <<- as.numeric(int_val)
      text_cols[[f]][row] <<- if (has_string) string_val else int_to_string(int_val)
    } else if (type %in% c(0x02, 0x06)) {
      numeric_cols[[f]][row] <<- as.numeric(double_val)
      text_cols[[f]][row] <<- if (has_string) string_val else double_to_string(double_val)
    } else if (type == 0x04) {
      text_cols[[f]][row] <<- string_val
    }
  }

  expected_indices <- sum(vapply(fields, function(field) field$bit_width > 0, logical(1))) * n_rows
  if (length(indices) != expected_indices) {
    stop("Metadata indices buffer length does not match field/row count")
  }

  index_pos <- 1L
  for (row in seq_len(n_rows)) {
    for (f in seq_len(n_fields)) {
      field <- fields[[f]]
      symbols <- field$symbols
      if (field$bit_width == 0) {
        if (nrow(symbols) > 0) {
          assign_symbol(row, f, symbols[1, , drop = FALSE])
        }
        next
      }

      idx <- indices[index_pos]
      index_pos <- index_pos + 1L

      if (idx == -2L) {
        next
      }

      if (idx < 0L || idx + 1L > nrow(symbols)) {
        next
      }

      assign_symbol(row, f, symbols[idx + 1L, , drop = FALSE])
    }
  }

  if (index_pos - 1L != length(indices)) {
    stop("Not all metadata indices were consumed during reconstruction")
  }

  col_names <- vapply(fields, function(field) field$field_name, character(1))

  data_cols <- vector("list", n_fields)
  text_out_cols <- vector("list", n_fields)

  for (f in seq_len(n_fields)) {
    field <- fields[[f]]
    symbols <- field$symbols
    types <- symbols$type
    has_int <- any(types %in% c(0x01, 0x05))
    has_double <- any(types %in% c(0x02, 0x06))
    has_dual <- any(types %in% c(0x05, 0x06))
    has_pure_string <- any(types == 0x04)
    has_dual_with_string <- any(types %in% c(0x05, 0x06) & !is.na(symbols$string) & nzchar(symbols$string))
    has_numeric <- has_int || has_double || has_dual
    has_any_string <- has_pure_string || has_dual_with_string
    prefer_numeric_here <- prefer_numeric_duals && has_dual && !has_pure_string
    use_character <- has_pure_string || (has_any_string && !prefer_numeric_here) || !has_numeric

    data_col <- if (!use_character && has_numeric) numeric_cols[[f]] else text_cols[[f]]
    data_cols[[f]] <- maybe_coerce_temporal(data_col)
    text_out_cols[[f]] <- text_cols[[f]]
  }

  data_cols <- setNames(data_cols, col_names)
  text_out_cols <- setNames(text_out_cols, col_names)

  data_df <- as.data.frame(data_cols, stringsAsFactors = FALSE, check.names = FALSE)
  text_df <- as.data.frame(text_out_cols, stringsAsFactors = FALSE, check.names = FALSE)

  if (length(temporal_columns)) {
    data_df <- qvdr:::coerce_temporal_columns(data_df, temporal_columns)
  }

  list(data = data_df, text = text_df)
}

sample_qvd_dir <- function() {
  local_dir <- testthat::test_path("..", "..", "inst", "sample_qvd")
  if (dir.exists(local_dir)) {
    return(local_dir)
  }

  system_dir <- system.file("sample_qvd", package = "qvdr")
  if (!nzchar(system_dir)) {
    testthat::skip("sample QVD assets are not available")
  }
  system_dir
}

df_to_character <- function(df) {
  as.data.frame(
    lapply(df, function(col) {
      out <- as.character(col)
      out[is.na(col)] <- NA_character_
      out
    }),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

test_that("read_qvd validates inputs", {
  expect_error(read_qvd(character()), "must be a single string")
  expect_error(read_qvd(NA_character_), "must be a single string")
})

test_that("AAPL.qvd decodes to the sample CSV", {
  sample_dir <- sample_qvd_dir()
  qvd_path <- file.path(sample_dir, "AAPL.qvd")
  csv_path <- file.path(sample_dir, "AAPL.csv")

  temporal_cols <- 1L
  loaded <- read_qvd(qvd_path, include_indices = TRUE, temporal_columns = temporal_cols)
  reference <- decode_qvd_reference(loaded, prefer_numeric_duals = TRUE, temporal_columns = temporal_cols)

  expect_equal(loaded$data, reference$data)
  expect_equal(loaded$text_data, reference$text)
  expect_s3_class(loaded$data$Date, "Date")

  csv <- read.csv(csv_path, stringsAsFactors = FALSE, check.names = FALSE)
  csv$Date <- as.Date(csv$Date)
  csv <- csv[colnames(reference$data)]

  expect_equal(reference$data, csv, tolerance = 1e-12)
  # expect_equal(reference$text, df_to_character(csv))

  loaded_text_pref <- read_qvd(qvd_path, prefer_numeric_duals = FALSE, include_indices = TRUE, temporal_columns = temporal_cols)
  reference_text_pref <- decode_qvd_reference(loaded_text_pref, prefer_numeric_duals = FALSE, temporal_columns = temporal_cols)

  expect_equal(loaded_text_pref$data, reference_text_pref$data)
  expect_equal(loaded_text_pref$text_data, reference_text_pref$text)
})

test_that("test_qvd.qvd uses row-major indices", {
  sample_dir <- sample_qvd_dir()
  qvd_path <- file.path(sample_dir, "test_qvd.qvd")

  loaded <- read_qvd(qvd_path, include_indices = TRUE)
  reference <- decode_qvd_reference(loaded, prefer_numeric_duals = TRUE)

  expect_equal(loaded$data, reference$data)
  expect_equal(loaded$text_data, reference$text)

  loaded_text_pref <- read_qvd(qvd_path, prefer_numeric_duals = FALSE, include_indices = TRUE)
  reference_text_pref <- decode_qvd_reference(loaded_text_pref, prefer_numeric_duals = FALSE)

  expect_equal(loaded_text_pref$data, reference_text_pref$data)
  expect_equal(loaded_text_pref$text_data, reference_text_pref$text)
})

test_that("test_qvd_null.qvd retains null markers", {
  sample_dir <- sample_qvd_dir()
  qvd_path <- file.path(sample_dir, "test_qvd_null.qvd")

  loaded <- read_qvd(qvd_path, include_indices = TRUE)
  reference <- decode_qvd_reference(loaded, prefer_numeric_duals = TRUE)

  expect_equal(loaded$data, reference$data)
  expect_equal(loaded$text_data, reference$text)

  loaded_text_pref <- read_qvd(qvd_path, prefer_numeric_duals = FALSE, include_indices = TRUE)
  reference_text_pref <- decode_qvd_reference(loaded_text_pref, prefer_numeric_duals = FALSE)

  expect_equal(loaded_text_pref$data, reference_text_pref$data)
  expect_equal(loaded_text_pref$text_data, reference_text_pref$text)
})
