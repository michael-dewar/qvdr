#' Read a QVD file into R
#'
#' @param path Path to the `.qvd` file.
#' @param prefer_numeric_duals Logical flag indicating whether dual-valued
#'   fields (with both numeric and textual representations) should default to
#'   their numeric values in the main data frame. When `FALSE`, dual-valued
#'   fields default to their textual representation. Regardless of the choice,
#'   textual values are always available in the `text_data` component of the
#'   result.
#' @param include_indices Logical flag indicating whether to include the raw
#'   symbol table indices from the underlying reader in the metadata. These are
#'   rarely needed but expose the entirety of the original C++ data structure.
#' @param temporal_columns Optional integer vector identifying columns in the
#'   returned `data` frame that should be converted from Qlik's numeric day
#'   offsets into R date or datetime objects. When supplied, integers are
#'   interpreted as `Date`s and fractional values as UTC `POSIXct` timestamps.
#'   When omitted, all decoded fields remain in their native numeric form.
#'
#' @return A list with three elements:
#'   \describe{
#'     \item{`metadata`}{Structured metadata mirroring the information exposed
#'       by the qvdreader library, including table header details, field
#'       definitions, symbol tables, and optional raw indices.}
#'     \item{`data`}{A base `data.frame` containing the decoded field values.
#'       Column types are inferred automatically, with an option to prefer
#'       numeric representations for dual-valued fields.}
#'     \item{`text_data`}{A `data.frame` of the same shape where all values are
#'       represented textually. This is particularly useful for preserving the
#'       textual side of dual-valued columns.}
#'   }
#'
#' @examples
#' \dontrun{
#' qvd <- read_qvd("/path/to/file.qvd")
#' str(qvd$data)
#' str(qvd$metadata$table_header$fields)
#' }
#'
#' @export
read_qvd <- function(path,
                     prefer_numeric_duals = TRUE,
                     include_indices = FALSE,
                     temporal_columns = integer()) {
  if (!is.character(path) || length(path) != 1L || is.na(path)) {
    stop("`path` must be a single string.", call. = FALSE)
  }

  path <- normalizePath(path, mustWork = TRUE)

  result <- read_qvd_cpp(path, prefer_numeric_duals, include_indices)

  if (length(temporal_columns)) {
    if (!is.numeric(temporal_columns)) {
      stop("`temporal_columns` must be an integer vector", call. = FALSE)
    }
    idx <- as.integer(temporal_columns)
    idx <- idx[!is.na(idx)]
    if (length(idx)) {
      result$data <- coerce_temporal_columns(result$data, idx)
    }
  }

  result
}

qlik_numeric_to_r <- function(x) {
  if (!is.numeric(x)) {
    warning("Attempted to coerce a non-numeric column; skipping.", call. = FALSE)
    return(x)
  }

  if (all(is.na(x))) {
    return(as.Date(x, origin = "1899-12-30"))
  }

  tol <- 1e-8
  has_fraction <- any(abs(x - round(x)) > tol, na.rm = TRUE)

  if (has_fraction) {
    secs <- (x - 25569) * 86400
    secs[is.na(x)] <- NA_real_
    as.POSIXct(secs, origin = "1970-01-01", tz = "UTC")
  } else {
    as.Date(x, origin = "1899-12-30")
  }
}

coerce_temporal_columns <- function(data, indices) {
  n_cols <- ncol(data)
  for (col in unique(indices)) {
    if (col < 1L || col > n_cols) {
      warning("Ignoring temporal column index outside data range: ", col,
              call. = FALSE)
      next
    }
    data[[col]] <- qlik_numeric_to_r(data[[col]])
  }
  data
}
