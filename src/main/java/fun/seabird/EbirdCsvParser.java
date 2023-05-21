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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class EbirdCsvParser 
{
	private static final Logger logger = LoggerFactory.getLogger(EbirdCsvParser.class);
	
	public enum ParseMode {single,parallel}
	
	public enum PreSort {none,obsDt}	
	
	private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("hh:mm a");

	private static final DateTimeFormatter csvDtf = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm a");
	
	private static final AtomicInteger linesProcessed = new AtomicInteger(0);
	
	static LocalDateTime parseSubDate(CSVRecord record)
	{
		if (record.getRecordNumber() == 1l)
            return LocalDateTime.MIN;        
		
		String obsTimeStr = record.get(12);
        if (StringUtils.isBlank(obsTimeStr))
            obsTimeStr = "12:00 AM";
		
		return LocalDateTime.parse(record.get(11) + " " + obsTimeStr, csvDtf);
	}
	
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
	    row.setDate(LocalDate.parse(record.get(11))); // Assuming the date format is ISO-8601 (yyyy-MM-dd)
	    
	    String timeStr = record.get(12);
	    if (!StringUtils.isEmpty(timeStr))
	    	row.setTime(LocalTime.parse(timeStr,timeFormatter)); // Assuming the time format is ISO-8601 (HH:mm:ss)
	    
	    row.setProtocol(record.get(13));
	    
		if (!record.get(14).isEmpty())
			row.setDuration(Integer.parseInt(record.get(14)));
		else
			row.setDuration(0);
	    
		row.setCompleteChecklist(record.get(15).equals("1"));

	    if (record.size() > 16 && !record.get(16).isEmpty()) {
	        row.setDistanceKm(Double.parseDouble(record.get(16)));
	    }

	    if (record.size() > 17 && !record.get(17).isEmpty()) {
	        row.setAreaHa(Double.parseDouble(record.get(17)));
	    }

	    if (record.size() > 18 && !record.get(18).isEmpty())
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
	 * Parses eBird CSV file using Apache Commons CSV library, and processes each line in parallel.
	 * 
	 * @param csvFile The path to the CSV file to be parsed.
	 * @throws IOException If an I/O error occurs while reading the CSV file.
	 */
	public static void parseCsv(Path csvFile,ParseMode mode,PreSort preSort,Consumer<EbirdCsvRow> rowProcessor) throws IOException
	{
		logger.info("Parsing " + csvFile + "...");
		
		linesProcessed.set(0);
		
		try (Reader fileReader = Files.newBufferedReader(csvFile);
				CSVParser csvParser = new CSVParser(fileReader,
						CSVFormat.DEFAULT.builder().setSkipHeaderRecord(true).build())) {

			StopWatch stopwatch = StopWatch.createStarted();
			
			Iterable<CSVRecord> records;
			if (PreSort.obsDt == preSort)
			{
				// Read all lines and sort by date and time columns
				List<CSVRecord> recordsList = csvParser.getRecords();	
				recordsList.sort(Comparator.comparing(EbirdCsvParser::parseSubDate));
				logger.info("Read " + (recordsList.size()-1) + " and sorted eBird observations in " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");
				records = recordsList;
			}
			else
				records = csvParser;
			
			Consumer<CSVRecord> csvRecordConsumer = new Consumer<CSVRecord>() {
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
				case parallel:
					Flux.fromIterable(records).parallel().runOn(Schedulers.parallel()).sequential(25000).doOnNext(csvRecordConsumer).then().block();
					break;
				case single:
					Flux.fromIterable(records).doOnNext(csvRecordConsumer).then().block();
					break;
			}

			stopwatch.stop();
			logger.info("Processed " + linesProcessed.get() + " eBird observations in " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");
		}
	}

}
