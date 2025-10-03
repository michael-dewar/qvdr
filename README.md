# qvdr 

`qvdr` wraps the vendored C++ [`qvdreader` library by Devin Smith](https://github.com/devinsmith/qvdreader) to let R users explore QVD data files without spinning up QlikView or Qlik Sense.

## ⚠️ Use at Your Own Risk

- This package ships with zero guarantees; expect breaking changes and rough edges.
- Assume production workloads will fail in surprising ways—rework your internal ETL pipelines instead of depending on this.
- Treat any output as advisory and re-run it through your established pipelines before acting on it.
- If you find yourself tempted to rely on this in a critical path, stop and invest that time evacuating either QVDs or R from the workflow entirely.

## Installation

Install the package

```r
# install.packages("remotes")
remotes::install_github("michael-dewar/qvdr")
```

## Usage

```r
read_rvd("path/to/my/data.qvd")
```

The output will have all of the metadata from the QVD, and two data frames.  This is because of Qlik's dual format structure in which a column like a date has both a text and numeric representation. The package does not attempt to guess what you want.

Dates should be converted to R format.  This package does not attempt to guess which columns are dates.  You can pass `read_qvd` a vector of integers `temporal_column` sthat represent the columns that you want converted to R datetimes.

