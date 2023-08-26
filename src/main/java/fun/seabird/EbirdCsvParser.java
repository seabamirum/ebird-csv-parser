package fun.seabird;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.StopWatch;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Slf4j
public abstract class EbirdCsvParser 
{
	public enum ParseMode {SINGLE_THREAD,MULTI_THREAD}
	
	public enum PreSort {NONE,DATE}	
	
	private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("hh:mm a");
	
	private static final AtomicInteger linesProcessed = new AtomicInteger(0);
	
	private static final int ROW_PREFETCH = 25000;
	
	/**
	 * Parses the date and time fields from a CSV record and returns a LocalDateTime object representing the combined datetime value. 
	 * If the time is not defined, assumes midnight.
	 *
	 * @param record The CSVRecord representing a single row of data in the CSV file.
	 * @return A LocalDateTime object representing the combined date and time parsed from the CSV record.
	 */
	private static LocalDateTime parseSubDate(CSVRecord record)
	{
		if (record.getRecordNumber() == 1l)
            return LocalDateTime.MIN; 
		
		LocalTime obsTime;
		if (record.get(12).isBlank())
			obsTime = LocalTime.MIDNIGHT;
		else
			obsTime = LocalTime.parse(record.get(12),timeFormatter);
		
		return LocalDate.parse(record.get(11)).atTime(obsTime);
	}
	
	/**
	 * Parses a single CSV record and constructs an EbirdCsvRow object from the record's fields.
	 *
	 * @param record The CSVRecord representing a single row of data in the CSV file.
	 * @return An EbirdCsvRow object constructed from the CSV record.
	 */
	private static EbirdCsvRow parseCsvLine(CSVRecord record) 
	{
	    if (record.getRecordNumber() == 1l)
	        return null; // skip the header

	    EbirdCsvRow row = new EbirdCsvRow();

	    row.setSubId(record.get(0));
	    row.setCommonName(record.get(1));
	    row.setSciName(record.get(2));
	    row.setTaxonOrder(Double.parseDouble(record.get(3)));
	    row.setCount(record.get(4));
	    row.setSubnat1Code(record.get(5));
	    row.setSubnat2Name(record.get(6));
	    row.setLocId(record.get(7));
	    row.setLocName(record.get(8));
	    row.setLat(Double.parseDouble(record.get(9)));
	    row.setLng(Double.parseDouble(record.get(10)));
	    row.setDate(LocalDate.parse(record.get(11))); // Date format is ISO-8601 (yyyy-MM-dd)
	    
	    if (!record.get(12).isBlank())
	    	row.setTime(LocalTime.parse(record.get(12),timeFormatter));
	    
	    row.setProtocol(record.get(13));
	    
		if (!record.get(14).isBlank())
			row.setDuration(Integer.parseInt(record.get(14)));
		else
			row.setDuration(0);
	    
		row.setCompleteChecklist(record.get(15).equals("1"));

	    if (record.size() > 16 && !record.get(16).isBlank()) {
	        row.setDistanceKm(Double.parseDouble(record.get(16)));
	    }

	    if (record.size() > 17 && !record.get(17).isBlank()) {
	        row.setAreaHa(Double.parseDouble(record.get(17)));
	    }

	    if (record.size() > 18 && !record.get(18).isBlank())
	    	row.setPartySize(Integer.parseInt(record.get(18)));
	    
	    if (record.size() > 19)
	    	row.setBreedingCode(record.get(19));

	    // Parsing the space-separated String into a List of Long values
	    if (record.size() > 22) {
	        String assetIdsString = record.get(22);
	        List<Long> assetIds = Arrays.stream(assetIdsString.split(" "))
	                .map(Long::parseLong)
	                .toList();
	        row.setAssetIds(assetIds);
	    }
	    
	    return row;
	}	
	
	/**
	 * Parses a CSV file and applies the given row processor to each CSV record.
	 * The parsing can be performed in single-threaded or multi-threaded mode,
	 * based on the provided ParseMode. Optionally, the CSV records can be pre-sorted
	 * based on the specified PreSort before processing.
	 *
	 * @param csvFile The path to the CSV file to be parsed.
	 * @param rowProcessor The consumer function to be applied to each parsed CSV row.
	 * @param mode The parsing mode: SINGLE_THREAD or MULTI_THREAD.
	 * @param preSort The pre-sorting option for the CSV records: null, PreSort.DATE, or PreSort.DEFAULT_SORT.
	 * @throws IOException If an I/O error occurs while reading the CSV file.
	 */
	public static final void parseCsv(Path csvFile,Consumer<EbirdCsvRow> rowProcessor,ParseMode mode,PreSort preSort) throws IOException
	{
		log.info("Parsing {} ...", csvFile);
		
		linesProcessed.set(0);
		
		try (Reader fileReader = Files.newBufferedReader(csvFile);
				CSVParser csvParser = new CSVParser(fileReader,
						CSVFormat.DEFAULT.builder().setSkipHeaderRecord(true).build())) {

			StopWatch stopwatch = StopWatch.createStarted();
			
			Flux<CSVRecord> recordsFlux;
			if (PreSort.DATE == preSort)
			{
				// Read all lines and sort by date and time columns
				List<CSVRecord> recordsList = csvParser.getRecords();	
				recordsList.sort(Comparator.comparing(EbirdCsvParser::parseSubDate));
				log.debug("Read and sorted {} eBird observations in {} seconds",recordsList.size()-1,stopwatch.getTime(TimeUnit.SECONDS));
				recordsFlux = Flux.fromIterable(recordsList);
			}
			else
				recordsFlux = Flux.fromIterable(csvParser);
			
			final Consumer<CSVRecord> csvRecordConsumer = new Consumer<CSVRecord>() {
			    @Override
			    public void accept(CSVRecord record) 
			    {
			    	EbirdCsvRow row = parseCsvLine(record);
			    	if (row == null) //header row
			    		return;  
			    	
			    	rowProcessor.accept(row);
					linesProcessed.getAndIncrement();
			    }
			};	
			
			switch (mode)
			{
				case MULTI_THREAD:
					recordsFlux.parallel().runOn(Schedulers.parallel()).sequential(ROW_PREFETCH).doOnNext(csvRecordConsumer).then().block();
					break;
				case SINGLE_THREAD:
					recordsFlux.doOnNext(csvRecordConsumer).then().block();
					break;
			}

			stopwatch.stop();
			
			log.info("Processed {} eBird observations in {} seconds",linesProcessed.get(),stopwatch.getTime(TimeUnit.SECONDS));
		}
	}
	
	/**
	 * Parses a CSV file using single-threaded mode and no pre-sorting, and applies the given row processor to each CSV record.
	 *
	 * @param csvFile The path to the CSV file to be parsed.
	 * @param rowProcessor The consumer function to be applied to each parsed CSV row.
	 * @throws IOException If an I/O error occurs while reading the CSV file.
	 */
	public static final void parseCsv(Path csvFile,Consumer<EbirdCsvRow> rowProcessor) throws IOException
	{
		parseCsv(csvFile,rowProcessor,ParseMode.SINGLE_THREAD,PreSort.NONE);		
	}
	
}
