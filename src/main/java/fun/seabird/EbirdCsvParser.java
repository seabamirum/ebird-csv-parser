package fun.seabird;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.StopWatch;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class EbirdCsvParser 
{	
	private EbirdCsvParser () {}
	
	public enum PreSort {NONE,DATE}	
	
	private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("hh:mm a");
	
	private static final AtomicInteger linesProcessed = new AtomicInteger(0);	
	
	/**
	 * Parses the date and time fields from a CSV record and returns a LocalDateTime object representing the combined datetime value. 
	 * If the time is not defined, assumes midnight.
	 *
	 * @param record The CsvRecord representing a single row of data in the CSV file.
	 * @return A LocalDateTime object representing the combined date and time parsed from the CSV record.
	 */
	private static LocalDateTime parseSubDate(CsvRecord record)
	{
		if (record.getStartingLineNumber()== 1l)
            return LocalDateTime.MIN; 
		
		LocalTime obsTime;
		if (record.getField(12).isBlank())
			obsTime = LocalTime.MIDNIGHT;
		else
			obsTime = LocalTime.parse(record.getField(12),timeFormatter);
		
		return LocalDate.parse(record.getField(11)).atTime(obsTime);
	}
	
	/**
	 * Parses a single CSV record and constructs an EbirdCsvRow object from the record's fields.
	 *
	 * @param record The CsvRecord representing a single row of data in the CSV file.
	 * @return An EbirdCsvRow object constructed from the CSV record.
	 */
	private static EbirdCsvRow parseCsvLine(CsvRecord record) 
	{
	    if (record.getStartingLineNumber() == 1l)
	        return null; // skip the header

	    EbirdCsvRow row = new EbirdCsvRow();

	    row.setSubId(record.getField(0));
	    row.setCommonName(record.getField(1));
	    row.setSciName(record.getField(2));
	    row.setTaxonOrder(Double.valueOf(record.getField(3)));
	    row.setCount(record.getField(4));
	    row.setSubnat1Code(record.getField(5));
	    row.setSubnat2Name(record.getField(6));
	    row.setLocId(record.getField(7));
	    row.setLocName(record.getField(8));
	    row.setLat(Double.valueOf(record.getField(9)));
	    row.setLng(Double.valueOf(record.getField(10)));
	    row.setDate(LocalDate.parse(record.getField(11))); // Date format is ISO-8601 (yyyy-MM-dd)
	    
	    if (!record.getField(12).isBlank())
	    	row.setTime(LocalTime.parse(record.getField(12),timeFormatter));
	    
	    row.setProtocol(record.getField(13));
	    
		if (!record.getField(14).isBlank())
			row.setDuration(Integer.valueOf(record.getField(14)));
		else
			row.setDuration(0);
	    
		row.setCompleteChecklist(record.getField(15).equals("1"));

		int fieldCount = record.getFieldCount();
	    if (fieldCount > 16 && !record.getField(16).isBlank()) {
	        row.setDistanceKm(Double.valueOf(record.getField(16)));
	    }

	    if (fieldCount > 17 && !record.getField(17).isBlank()) {
	        row.setAreaHa(Double.valueOf(record.getField(17)));
	    }

	    if (fieldCount > 18 && !record.getField(18).isBlank())
	    	row.setPartySize(Integer.valueOf(record.getField(18)));
	    
	    if (fieldCount > 19)
	    	row.setBreedingCode(record.getField(19));

	    // Parsing the space-separated String into a List of Long values
	    if (fieldCount > 22) {
	        String assetIdsString = record.getField(22);
	        List<Long> assetIds = Arrays.stream(assetIdsString.split(" "))
	                .map(Long::valueOf)
	                .toList();
	        row.setAssetIds(assetIds);
	    }
	    
	    return row;
	}	
	
	/**
	 * Parses a CSV file containing eBird observation data and processes each row according to the specified {@code rowProcessor}.
	 * The method allows for pre-sorting of data based on the observation date and supports processing in either single-threaded
	 * or multi-threaded mode as specified by the {@code mode} parameter.
	 *
	 * @param csvFile The path to the CSV file to be parsed. Must not be {@code null}.
	 * @param rowProcessor A {@link Consumer} that processes each parsed row of the CSV file. The consumer receives an instance
	 *                     of {@link EbirdCsvRow}, which represents the parsed data of each row. Must not be {@code null}.	 
	 * @param preSort Specifies the pre-sorting method to be applied to the CSV data before processing. If set to
	 *                {@link PreSort#DATE}, the data will be sorted by observation date prior to processing; otherwise, the
	 *                data is processed in the order it appears in the file.
	 * @throws IOException If an I/O error occurs while reading the CSV file.
	 */
	public static void parseCsv(Path csvFile,Consumer<EbirdCsvRow> rowProcessor,PreSort preSort) throws IOException
	{
		log.info("Parsing " + csvFile + " ...");
		
		linesProcessed.set(0);
		
		try (CsvReader<CsvRecord> csvParser = CsvReader.builder().ofCsvRecord(csvFile)) 
		{
			StopWatch stopwatch = StopWatch.createStarted();
			
			@SuppressWarnings("resource")
			Iterable<CsvRecord> recordsIterable = csvParser;
			if (PreSort.DATE == preSort)
			{
				// Read all lines and sort by date and time columns
				List<CsvRecord> recordsList = csvParser.stream().collect(Collectors.toList());
				recordsList.sort(Comparator.comparing(EbirdCsvParser::parseSubDate));
				log.debug("Read and sorted " + (recordsList.size()-1) + " eBird observations in " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");
				recordsIterable = recordsList;
			}				
			
			final Consumer<CsvRecord> CsvRecordConsumer = new Consumer<CsvRecord>() {
			    @Override
			    public void accept(CsvRecord record) 
			    {
			    	EbirdCsvRow row = parseCsvLine(record);
			    	if (row == null) //header row
			    		return;  
			    	
			    	rowProcessor.accept(row);
					linesProcessed.getAndIncrement();
			    }
			};	
			
			recordsIterable.forEach(CsvRecordConsumer);

			stopwatch.stop();
			
			log.info("Processed " + linesProcessed.get() + " eBird observations in " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");
		}
	}
	
	/**
	 * Parses a CSV file with no pre-sorting, and applies the given row processor to each CSV record.
	 *
	 * @param csvFile The path to the CSV file to be parsed.
	 * @param rowProcessor The consumer function to be applied to each parsed CSV row.
	 * @throws IOException If an I/O error occurs while reading the CSV file.
	 */
	public static void parseCsv(Path csvFile,Consumer<EbirdCsvRow> rowProcessor) throws IOException
	{
		parseCsv(csvFile,rowProcessor,PreSort.NONE);		
	}
	
}
