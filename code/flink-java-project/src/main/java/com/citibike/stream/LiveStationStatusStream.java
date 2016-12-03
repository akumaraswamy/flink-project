package com.citibike.stream;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.citibike.model.StationStatus;
import com.citibike.stream.source.StationStatusFeedSource;

/**
 * This class reads the live stream, filters based on bike availability,
 * publishes the filtered data into elastic search.
 * 
 * @author Aruna Kumaraswamy
 *
 */
public class LiveStationStatusStream {
	
	private static transient DateTimeFormatter timeFormatter =
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
	
	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	public static final String LIVE_STATION_STATUS = "liveStationStatus";

	public static void main(String[] args) throws Exception {
		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String url = params.getRequired("input");
		
		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minute are served in 1 second


		final StreamExecutionEnvironment env = StreamExecutionEnvironment.
										getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	    
	    //String url = args[0];
	    DataStream<StationStatus> liveStationFeed = env.addSource(
	    							new StationStatusFeedSource(url,maxEventDelay,servingSpeedFactor));
	    
	    				
	    DataStream<StationStatus> bikeAlerts = liveStationFeed
	    							.filter(new BikeAlertFilter()
	    							);
	    
	    Map<String, String> config = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "10");
		config.put("cluster.name", "elasticsearch");

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

		bikeAlerts.addSink(new ElasticsearchSink<StationStatus>(
				config,
				transports,
				new BikeAlertInserter()));
	    		
	    //bikeAlerts.print();
	
	    // run the alert pipeline
	 	env.execute("Station Status Alert");
	}
	
	public static class BikeAlertFilter implements FilterFunction<StationStatus> {

		@Override
		public boolean filter(StationStatus status) throws Exception {

			return (status.getAvailableBikes() < 2 && !status.isTestStation());
		}
	}

	public static class BikeAlertInserter
				implements ElasticsearchSinkFunction<StationStatus> {

				// construct index request
				@Override
				public void process(
						StationStatus record,
						RuntimeContext ctx,
						RequestIndexer indexer) {
				
					// construct JSON document to index
					Map<String, String> json = new HashMap<>();
					json.put("time", String.valueOf(getJodaTime(record.getLastCommunicationTime()).getMillis()));         // timestamp
					json.put("location", record.getLatitude()+","+record.getLongitude());  // lat,lon pair
					json.put("name", record.getStationName());      // station name
					json.put("cnt", String.valueOf(record.getAvailableBikes()));          // count
				
					IndexRequest rqst = Requests.indexRequest()
							.index("citi-bikes")        // index name
							.type("bike-alerts")  // mapping name
							.source(json);
				
					indexer.add(rqst);
				}
				private DateTime getJodaTime(String time){
					DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss a");
					DateTime dt = formatter.parseDateTime(time);
					return dt;
				}
	}
	
	
	
}
