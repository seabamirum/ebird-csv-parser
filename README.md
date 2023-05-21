# eBird CSV Parser

The eBird CSV Parser is a convenient tool for parsing downloaded eBird CSV files, which contain bird observation data. It allows for further manipulation and processing of the data in a structured manner. This tool is especially useful for working with large datasets and performing custom analyses.

## Features

- Parses eBird CSV files downloaded from [https://ebird.org/downloadMyData](https://ebird.org/downloadMyData).
- Ignores observation and checklist comments to avoid memory problems during parsing.
- Supports different parsing modes for flexibility and performance optimization.

## Usage

To use the eBird CSV Parser, you can call the `parseCsv` method with the appropriate parameters. Here are some example usages:

```java
EbirdCsvParser.parseCsv(csvFile, RouteLoader::parseCsvLine, ParseMode.SINGLE_THREAD, PreSort.DATE);
```

In this case, the CSV file is first sorted by date. This can be useful when you want to operate on a limited set of records and fetch checklists in order. Please note that sorting by date incurs a performance hit.


```java
EbirdCsvParser.parseCsv(msc.getCsvFile(), this::parseCsvLine, ParseMode.MULTI_THREAD, PreSort.NONE);
```

When using the MULTI_THREAD mode, ensure that your RowProcessor implementation can handle multiple threads and that any data structures used are concurrently modifiable.

## Contribution
Contributions to the eBird CSV Parser project are welcome! If you find any issues or have suggestions for improvements, please create an issue or submit a pull request.
