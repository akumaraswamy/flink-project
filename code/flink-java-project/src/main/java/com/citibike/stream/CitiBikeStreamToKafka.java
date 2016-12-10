package com.citibike.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import com.citibike.model.BikeRide;
import com.citibike.model.BikeRideSchema;
import com.citibike.stream.source.DataStreamSimulator;

/**
 * Citi Bike publishes historic rides as zip in Amazon S3
 * Download the zip and use it as input.
 * 
 * Output is streamed to Kafka topics.
 * 
 * @author akumara
 * 
 *
 */
public class CitiBikeStreamToKafka {
	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	public static final String RIDES_TABLE_SINK_TOPIC = "ridesToTableSink";
	public static final String RIDES_FOR_WINDOWING_TOPIC = "ridesToWindow";
	
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
				
		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minute are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<BikeRide> rides = env.addSource(new DataStreamSimulator(input, maxEventDelay, servingSpeedFactor));
		
		//Get Daily rides
	//	DataStream<BikeRide> filteredRides = rides.filter(new DailyTripFilter());
		
		//Send data to Data sink as-is
		rides.addSink(new FlinkKafkaProducer09<>(
				LOCAL_KAFKA_BROKER,
				RIDES_TABLE_SINK_TOPIC,
				new BikeRideSchema()));
		
		//Send to sink to analyze popular locations
		rides.addSink(new FlinkKafkaProducer09<>(
				LOCAL_KAFKA_BROKER,
				RIDES_FOR_WINDOWING_TOPIC,
				new BikeRideSchema()));
		
				 
		// run the cleansing pipeline
		env.execute("Bike Ride Cleansing");
	}
	
	/*public static class DailyTripFilter implements FilterFunction<BikeRide> {

		@Override
		public boolean filter(BikeRide ride) throws Exception {
			System.out.println(ride.startTime + " "+ride.endTime);
			
			Period period = new Period(ride.startTime,ride.endTime);
			return (period.getDays() == 0);
						
		}
	}*/
}
