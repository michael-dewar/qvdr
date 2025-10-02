#include <Rcpp.h>

#include <cmath>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include "qvdreader/QvdFile.h"
#include "qvdreader/QvdField.h"
#include "qvdreader/QvdSymbol.h"
#include "qvdreader/QvdTableHeader.h"

using Rcpp::CharacterVector;
using Rcpp::DataFrame;
using Rcpp::IntegerVector;
using Rcpp::List;
using Rcpp::Named;
using Rcpp::NumericVector;
using Rcpp::String;

namespace {

std::string double_to_string(double value) {
  std::ostringstream oss;
  oss.setf(std::ios::fixed);
  oss << std::setprecision(15) << value;
  std::string out = oss.str();
  // Trim trailing zeros and decimal point if not needed.
  size_t pos = out.find_last_not_of('0');
  if (pos != std::string::npos) {
    if (out[pos] == '.') {
      out.erase(pos);
    } else {
      out.erase(pos + 1);
    }
  }
  if (out.empty()) {
    return "0";
  }
  return out;
}

std::string int_to_string(int value) {
  return std::to_string(value);
}

void assign_symbol_to_row(NumericVector &numeric,
                          CharacterVector &text,
                          std::size_t row,
                          const QvdSymbol &sym) {
  switch (sym.Type) {
  case 0x01:
    numeric[row] = static_cast<double>(sym.IntValue);
    text[row] = int_to_string(sym.IntValue);
    break;
  case 0x02:
    numeric[row] = sym.DoubleValue;
    text[row] = double_to_string(sym.DoubleValue);
    break;
  case 0x04:
    text[row] = sym.StringValue;
    break;
  case 0x05:
    numeric[row] = static_cast<double>(sym.IntValue);
    if (!sym.StringValue.empty()) {
      text[row] = sym.StringValue;
    } else {
      text[row] = int_to_string(sym.IntValue);
    }
    break;
  case 0x06:
    numeric[row] = sym.DoubleValue;
    if (!sym.StringValue.empty()) {
      text[row] = sym.StringValue;
    } else {
      text[row] = double_to_string(sym.DoubleValue);
    }
    break;
  default:
    break;
  }
}

CharacterVector create_text_vector(std::size_t n_rows) {
  CharacterVector text(n_rows);
  for (std::size_t i = 0; i < n_rows; ++i) {
    text[i] = NA_STRING;
  }
  return text;
}

List make_dataframe(const List &columns, const CharacterVector &col_names,
                    std::size_t n_rows) {
  List df(columns);
  df.attr("names") = col_names;
  df.attr("class") = "data.frame";
  df.attr("row.names") = IntegerVector::create(NA_INTEGER,
                                                -static_cast<int>(n_rows));
  return df;
}

List symbols_to_dataframe(const std::vector<QvdSymbol> &symbols) {
  std::size_t n = symbols.size();
  IntegerVector type_vec(n);
  CharacterVector string_vec(n);
  IntegerVector int_vec(n);
  NumericVector double_vec(n);

  for (std::size_t i = 0; i < n; ++i) {
    const QvdSymbol &sym = symbols[i];
    type_vec[i] = sym.Type;

    if (!sym.StringValue.empty()) {
      string_vec[i] = sym.StringValue;
    } else {
      string_vec[i] = NA_STRING;
    }

    if (sym.Type == 0x01 || sym.Type == 0x05) {
      int_vec[i] = sym.IntValue;
    } else {
      int_vec[i] = NA_INTEGER;
    }

    if (sym.Type == 0x02 || sym.Type == 0x06) {
      double_vec[i] = sym.DoubleValue;
    } else if (sym.Type == 0x01 || sym.Type == 0x05) {
      double_vec[i] = static_cast<double>(sym.IntValue);
    } else {
      double_vec[i] = Rcpp::NumericVector::get_na();
    }
  }

  List df = List::create(Named("type") = type_vec,
                         Named("string") = string_vec,
                         Named("int") = int_vec,
                         Named("double") = double_vec);

  df.attr("class") = "data.frame";
  df.attr("row.names") = IntegerVector::create(NA_INTEGER,
                                                -static_cast<int>(n));
  return df;
}

} // namespace

// [[Rcpp::export]]
List read_qvd_cpp(const std::string &path, bool prefer_numeric_duals = true,
                  bool include_indices = false) {
  QvdFile file;
  if (!file.Load(path.c_str())) {
    Rcpp::stop("Failed to load QVD file: " + path);
  }

  QvdTableHeader header = file.GetTableHeader();
  std::size_t n_fields = header.Fields.size();
  std::size_t n_rows = header.NoOfRecords;

  CharacterVector col_names(n_fields);
  List data_columns(n_fields);
  List text_columns(n_fields);
  List fields_meta(n_fields);

  const std::vector<int> &indices = header.Indices;

  struct FieldDecodeState {
    NumericVector numeric;
    CharacterVector text;
    bool use_character;
    bool has_numeric_symbol;
  };

  std::vector<FieldDecodeState> field_states;
  field_states.reserve(n_fields);

  for (std::size_t f = 0; f < n_fields; ++f) {
    const QvdField &field = header.Fields[f];
    col_names[f] = field.FieldName;

    NumericVector numeric(n_rows, Rcpp::NumericVector::get_na());
    CharacterVector text = create_text_vector(n_rows);

    bool field_has_int_symbol = false;
    bool field_has_double_symbol = false;
    bool field_has_pure_string_symbol = false;
    bool field_has_dual_symbol = false;
    bool field_has_dual_with_string = false;

    for (std::size_t s = 0; s < field.Symbols.size(); ++s) {
      const QvdSymbol &sym = field.Symbols[s];
      switch (sym.Type) {
      case 0x01:
        field_has_int_symbol = true;
        break;
      case 0x02:
        field_has_double_symbol = true;
        break;
      case 0x04:
        field_has_pure_string_symbol = true;
        break;
      case 0x05:
        field_has_int_symbol = true;
        field_has_dual_symbol = true;
        if (!sym.StringValue.empty()) {
          field_has_dual_with_string = true;
        }
        break;
      case 0x06:
        field_has_double_symbol = true;
        field_has_dual_symbol = true;
        if (!sym.StringValue.empty()) {
          field_has_dual_with_string = true;
        }
        break;
      default:
        break;
      }
    }

    bool field_has_numeric_symbol = field_has_int_symbol || field_has_double_symbol ||
                                    field_has_dual_symbol;
    bool field_has_any_string = field_has_pure_string_symbol || field_has_dual_with_string;
    bool prefer_numeric_here = prefer_numeric_duals && field_has_dual_symbol && !field_has_pure_string_symbol;
    bool use_character = field_has_pure_string_symbol || (field_has_any_string && !prefer_numeric_here) || !field_has_numeric_symbol;

    if (n_rows > 0 && field.BitWidth == 0 && !field.Symbols.empty()) {
      const QvdSymbol &sym = field.Symbols[0];
      for (std::size_t row = 0; row < n_rows; ++row) {
        assign_symbol_to_row(numeric, text, row, sym);
      }
    }

    field_states.push_back(FieldDecodeState{numeric, text, use_character, field_has_numeric_symbol});

    List number_format = List::create(Named("type") = field.Type,
                                      Named("nDec") = static_cast<int>(field.nDec),
                                      Named("UseThou") = static_cast<int>(field.UseThou),
                                      Named("Dec") = field.Dec,
                                      Named("Thou") = field.Thou);

    List field_info = List::create(Named("field_name") = field.FieldName,
                                   Named("bit_offset") = static_cast<int>(field.BitOffset),
                                   Named("bit_width") = static_cast<int>(field.BitWidth),
                                   Named("bias") = field.Bias,
                                   Named("no_of_symbols") = static_cast<int>(field.NoOfSymbols),
                                   Named("offset") = static_cast<int>(field.Offset),
                                   Named("length") = static_cast<int>(field.Length),
                                   Named("number_format") = number_format,
                                   Named("symbols") = symbols_to_dataframe(field.Symbols));

    fields_meta[f] = field_info;
  }

  std::size_t index_pos = 0;
  for (std::size_t row = 0; row < n_rows; ++row) {
    for (std::size_t f = 0; f < n_fields; ++f) {
      const QvdField &field = header.Fields[f];
      if (field.BitWidth == 0) {
        continue;
      }
      if (index_pos >= indices.size()) {
        Rcpp::stop("Index buffer underrun while decoding field '" +
                   field.FieldName + "'");
      }
      int idx = indices[index_pos++];
      if (idx == -2) {
        continue;
      }
      if (idx < 0 || static_cast<std::size_t>(idx) >= field.Symbols.size()) {
        continue;
      }
      assign_symbol_to_row(field_states[f].numeric, field_states[f].text,
                           row, field.Symbols[idx]);
    }
  }

  if (index_pos != indices.size()) {
    Rcpp::warning("Not all index values were consumed while decoding the data.");
  }

  for (std::size_t f = 0; f < n_fields; ++f) {
    if (!field_states[f].use_character && field_states[f].has_numeric_symbol) {
      data_columns[f] = field_states[f].numeric;
    } else {
      data_columns[f] = field_states[f].text;
    }
    text_columns[f] = field_states[f].text;
  }

  List data_df = make_dataframe(data_columns, col_names, n_rows);
  List text_df = make_dataframe(text_columns, col_names, n_rows);

  List lineages(header.Lineages.size());
  for (std::size_t i = 0; i < header.Lineages.size(); ++i) {
    const QvdLineageInfo &lin = header.Lineages[i];
    lineages[i] = List::create(Named("discriminator") = lin.Discriminator,
                               Named("statement") = lin.Statement);
  }

  List table_header = List::create(
      Named("qv_build_no") = header.QvBuildNo,
      Named("creator_doc") = header.CreatorDoc,
      Named("create_utc_time") = header.CreateUtcTime,
      Named("source_file_size") = header.SourceFileSize,
      Named("table_name") = header.TableName,
      Named("record_byte_size") = static_cast<int>(header.RecordByteSize),
      Named("no_of_records") = static_cast<int>(header.NoOfRecords),
      Named("offset") = static_cast<int>(header.Offset),
      Named("length") = static_cast<int>(header.Length),
      Named("lineage") = lineages,
      Named("fields") = fields_meta);

  List metadata = List::create(Named("table_header") = table_header);

  if (include_indices) {
    IntegerVector idx_vec(indices.size());
    for (std::size_t i = 0; i < indices.size(); ++i) {
      idx_vec[i] = indices[i];
    }
    metadata["indices"] = idx_vec;
  }

  return List::create(Named("metadata") = metadata,
                      Named("data") = data_df,
                      Named("text_data") = text_df);
}
