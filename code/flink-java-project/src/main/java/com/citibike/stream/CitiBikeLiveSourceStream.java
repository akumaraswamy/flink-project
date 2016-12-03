package com.citibike.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

import com.citibike.model.StationStatus;
import com.citibike.model.StationStatusSchema;
import com.citibike.stream.source.StationStatusFeedSource;

public class CitiBikeLiveSourceStream {
	
	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	public static final String LIVE_STATION_TOPIC = "liveStationFeed3";
	
	public static void main(String[] args) throws Exception {
		// read parameters
		//ParameterTool params = ParameterTool.fromArgs(args);
		//String urlInput = params.getRequired("input");
		String urlInput = "https://feeds.citibikenyc.com/stations/stations.json";
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		// Station Feed
		DataStream<StationStatus> liveStationFeed = env
				.addSource(new StationStatusFeedSource(urlInput));

		//station feed to table sink
		liveStationFeed.addSink(new FlinkKafkaProducer09<>(
				LOCAL_KAFKA_BROKER,
				LIVE_STATION_TOPIC,
				new StationStatusSchema()));
	
		// run the alert pipeline
		env.execute("Citi Bike Source Stream");
	}

}
