# eBird CSV Parser

The eBird CSV Parser is a convenient tool for parsing downloaded eBird CSV files, which contain bird observation data. It allows for further manipulation and processing of the data in a structured manner. This tool is especially useful for working with large datasets and performing custom analyses.

## Features

- Parses eBird CSV files downloaded from [https://ebird.org/downloadMyData](https://ebird.org/downloadMyData).
- Ignores observation and checklist comments to avoid memory problems during parsing.

## Usage

To use the eBird CSV Parser, all you need to do is implement your own row processor method and call the `parseCsv` method with the appropriate parameters. Here are some example usages:

```java

private void processCsvRow(EbirdCsvRow row) 
{
  String subId = row.getSubId();
  //the rest of your implementation here
}

EbirdCsvParser.parseCsv(csvPath,this::processCsvRow,PreSort.DATE);
```

In this case, the CSV file is first sorted by date. This can be useful when you want to operate on a limited set of records and fetch checklists in order. Please note that sorting by date incurs a performance hit.


```java
EbirdCsvParser.parseCsv(csvPath,this::processCsvRow,PreSort.NONE);
```

## Contribution
Contributions to the eBird CSV Parser project are welcome! If you find any issues or have suggestions for improvements, please create an issue or submit a pull request.
