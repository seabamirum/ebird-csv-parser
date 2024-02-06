package fun.seabird;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded=true)
@ToString
public class EbirdCsvRow 
{
	private String subId;
	private String commonName;
	@Include private String sciName;
	private Double taxonOrder;
	private String count;
	private String subnat1Code;
	private String subnat2Name;
	@Include private String locId;
	private String locName;
	private Double lat;
	private Double lng;
	private LocalDate date;
	private LocalTime time;
	private String protocol;
	private Integer duration;
	private Boolean completeChecklist;
	private Double distanceKm;
	private Double areaHa;
	private Integer partySize;
	private String breedingCode;
	
	//space-separated String in the CSV
	private List<Long> assetIds = new ArrayList<>();
	
	@Include
	public LocalDateTime dateTime()
	{
		if (time == null)
			return date.atStartOfDay();
		
		return date.atTime(time);
	}
	
}
