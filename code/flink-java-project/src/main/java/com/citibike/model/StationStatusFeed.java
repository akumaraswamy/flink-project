package com.citibike.model;

import java.util.List;

import org.joda.time.DateTime;

import com.citibike.utils.JSONMarshaller;

/**
 * This class models the JSON for the live data feed.
 * 
 * @author Aruna Kumaraswamy
 *
 */
public class StationStatusFeed {
	
	public String executionTime;
	public List<StationStatus> stationBeanList;
	
	public StationStatusFeed(){}
	
	public StationStatusFeed(String line){
		try {
			fromString(line);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void fromString(String line) throws Exception{
		JSONMarshaller<StationStatusFeed> marshaller = 
				new JSONMarshaller<>();
		StationStatusFeed liveFeed = marshaller.fromJSON(line, 
								StationStatusFeed.class);
		this.executionTime = liveFeed.executionTime;
		this.stationBeanList = liveFeed.stationBeanList;
		
	}
	
	public String toJSON() throws Exception{
		JSONMarshaller<StationStatusFeed> marshaller = 
				new JSONMarshaller<>();
		 return marshaller.toJSON(stationBeanList);
	}

	
}
