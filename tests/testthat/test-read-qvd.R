sample_qvd_path <- function(filename) {
  local_candidate <- testthat::test_path("..", "..", "inst", "sample_qvd", filename)
  if (file.exists(local_candidate)) {
    return(local_candidate)
  }

  system_candidate <- system.file("sample_qvd", filename, package = "qvdr")
  if (!nzchar(system_candidate)) {
    testthat::skip("sample QVD assets are not available")
  }
  system_candidate
}

test_that("read_qvd validates inputs", {
  expect_error(read_qvd(character()), "must be a single string")
  expect_error(read_qvd(NA_character_), "must be a single string")
})

test_that("AAPL.qvd yields expected data slices", {
  qvd_path <- sample_qvd_path("AAPL.qvd")

  loaded <- read_qvd(qvd_path, temporal_columns = 1L)
  data_df <- as.data.frame(loaded[["data"]], stringsAsFactors = FALSE)
  text_df <- as.data.frame(loaded[["text_data"]], stringsAsFactors = FALSE)

  expect_s3_class(data_df$Date, "Date")
  expect_equal(nrow(data_df), 2746L)
  expect_identical(names(data_df), c("Date", "Open", "High", "Dividends", "Low", "Close", "Volume", "Stock Splits"))

  expected_data <- data.frame(
    Date = as.Date(c("2010-01-04", "2010-01-05", "2010-01-06", "2010-01-07", "2010-01-08")),
    Open = c(6.522157623622897, 6.557910119774437, 6.551188676404236, 6.470819029841401, 6.4265075425282046),
    High = c(6.55485543686017, 6.588163169855102, 6.577163816485664, 6.478458896588904, 6.478457811704492),
    Dividends = rep(0, 5),
    Low = c(6.490070999717296, 6.516655663843725, 6.440260662372158, 6.388310508986188, 6.388615033816772),
    Close = c(6.539881706237793, 6.551187038421631, 6.446983337402344, 6.435065269470215, 6.477846622467041),
    Volume = c(493729600, 601904800, 552160000, 477131200, 447610800),
    `Stock Splits` = rep(0, 5),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  expected_text <- data.frame(
    Date = c("2010-01-04", "2010-01-05", "2010-01-06", "2010-01-07", "2010-01-08"),
    Open = c("6.522157623622897", "6.557910119774437", "6.551188676404236", "6.470819029841401", "6.4265075425282046"),
    High = c("6.55485543686017", "6.588163169855102", "6.577163816485664", "6.478458896588904", "6.478457811704492"),
    Dividends = rep("0.0", 5),
    Low = c("6.490070999717296", "6.516655663843725", "6.440260662372158", "6.388310508986188", "6.388615033816772"),
    Close = c("6.539881706237793", "6.551187038421631", "6.446983337402344", "6.435065269470215", "6.477846622467041"),
    Volume = c("493729600", "601904800", "552160000", "477131200", "447610800"),
    `Stock Splits` = rep("0.0", 5),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  observe_rows <- seq_len(nrow(expected_data))
  expect_equal(data_df[observe_rows, ], expected_data, tolerance = 1e-12)
  expect_equal(text_df[observe_rows, ], expected_text)

  expect_equal(loaded$metadata$table_header$table_name, "Stock")
})

test_that("AAPL.qvd respects prefer_numeric_duals = FALSE", {
  qvd_path <- sample_qvd_path("AAPL.qvd")

  loaded <- read_qvd(qvd_path, prefer_numeric_duals = FALSE)
  data_list <- loaded[["data"]]
  text_list <- loaded[["text_data"]]

  is_character_col <- vapply(names(data_list), function(name) is.character(data_list[[name]]), logical(1))
  expect_true(all(is_character_col))
  expect_identical(data_list[["Open"]][1:3], text_list[["Open"]][1:3])
})

test_that("test_qvd.qvd decodes the fixed dataset", {
  qvd_path <- sample_qvd_path("test_qvd.qvd")

  loaded <- read_qvd(qvd_path, include_indices = TRUE)
  data_df <- as.data.frame(loaded[["data"]], stringsAsFactors = FALSE)
  text_df <- as.data.frame(loaded[["text_data"]], stringsAsFactors = FALSE)

  expected_data <- data.frame(
    `TEST.Month` = 1:12,
    `TEST.double` = c(1.2, 10, 64, 0, 0, 0, 1, 213.95625, 2, 3, 5, 1000),
    `TEST.big` = c(
      "3213821398129038", "1241264654654", "13213154565465464", "8478784",
      "37898975865", "999999999", "765765756865865", "54455435435643",
      "765765775", "4354354343", "240", "64"
    ),
    `TEST.Quarter` = rep(c("Q1", "Q2", "Q3", "Q4"), each = 3),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  expected_text <- data.frame(
    `TEST.Month` = as.character(1:12),
    `TEST.double` = c("1.2", "10.0", "64", "0", "0", "0", "1", "213.95625", "2", "3", "5", "1000"),
    `TEST.big` = c(
      "3213821398129038", "1241264654654", "13213154565465464", "8478784",
      "37898975865", "999999999", "765765756865865", "54455435435643",
      "765765775", "4354354343", "240", "64"
    ),
    `TEST.Quarter` = rep(c("Q1", "Q2", "Q3", "Q4"), each = 3),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  expect_equal(data_df, expected_data, tolerance = 1e-12)
  expect_equal(text_df, expected_text)
  expect_true(is.integer(loaded$metadata$indices))
  expect_equal(length(loaded$metadata$indices), nrow(data_df) * ncol(data_df))
})

test_that("test_qvd_null.qvd retains null markers", {
  qvd_path <- sample_qvd_path("test_qvd_null.qvd")

  loaded <- read_qvd(qvd_path)
  data_df <- as.data.frame(loaded[["data"]], stringsAsFactors = FALSE)
  text_df <- as.data.frame(loaded[["text_data"]], stringsAsFactors = FALSE)

  expected_data <- data.frame(
    Month = 1:12,
    some_null = c(1.2, 10, 64, NA, NA, NA, 1, 213.95625, 2, 3, 5, 1000),
    Quarter = rep(c("Q1", "Q2", "Q3", "Q4"), each = 3),
    `all Null` = rep(NA_character_, 12),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  expected_text <- data.frame(
    Month = as.character(1:12),
    some_null = c("1.2", "10.0", "64", NA, NA, NA, "1", "213.95625", "2", "3", "5", "1000"),
    Quarter = rep(c("Q1", "Q2", "Q3", "Q4"), each = 3),
    `all Null` = rep(NA_character_, 12),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  expect_equal(data_df, expected_data, tolerance = 1e-12)
  expect_equal(text_df, expected_text)
  expect_true(anyNA(data_df$some_null))
  expect_true(all(is.na(text_df$`all Null`)))
})
