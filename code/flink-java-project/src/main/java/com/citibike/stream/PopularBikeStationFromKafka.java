package com.citibike.stream;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;

import com.citibike.model.BikeRide;
import com.citibike.model.BikeRideSchema;

/**
 * PopularBikeStationFromKafka reads the daily trips from kafka and based on the ride count, 
 * it filters the popular locations. Threshold for the ride is set to 20.
 * 
 * A sliding window into the stream data is set to 15 minute of streaming with 5 minute slide.
 * 
 * @author akumara
 *
 */
public class PopularBikeStationFromKafka {
	
	private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	private static final String RIDE_SPEED_GROUP = "rideSpeedGroup";
	public static final String RIDES_FOR_WINDOWING_TOPIC = "ridesToWindow";
	
	private static final int MAX_EVENT_DELAY = 60;
	
	public static void main(String[] args) throws Exception {

		final int popThreshold = 25; // threshold for popular places

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000);

		// configure the Kafka consumer
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
		kafkaProps.setProperty("group.id", RIDE_SPEED_GROUP);
		// always read the Kafka topic from the start
		kafkaProps.setProperty("auto.offset.reset", "earliest");

		// create a Kafka consumer
		FlinkKafkaConsumer09<BikeRide> consumer = new FlinkKafkaConsumer09<>(
				RIDES_FOR_WINDOWING_TOPIC,
				new BikeRideSchema(),
				kafkaProps);
		// assign a timestamp extractor to the consumer
		consumer.assignTimestampsAndWatermarks(new BikeRideTSExtractor());
				
		// create a TaxiRide data stream
		DataStream<BikeRide> rides = env.addSource(consumer);
		
		// find popular places
				DataStream<Tuple4<String, Long, Boolean, Integer>> popularPlaces = rides
						// match ride to grid cell and event type (start or end)
						.map(new StationFlatMap())
						// partition by stationame and start indicator
						.keyBy(0, 1)
						// build sliding window
						.timeWindow(Time.minutes(15), Time.minutes(5))
						// count ride events in window
						.apply(new RideCounter())
						// filter by popularity threshold
						.filter(new FilterFunction<Tuple4<String, Long, Boolean, Integer>>() {
							@Override
							public boolean filter(Tuple4<String, Long, Boolean, Integer> count) throws Exception {
								return count.f3 >= popThreshold;
							}
						});
						// map grid cell to coordinates
						//.map(new GridToCoordinates());

		popularPlaces.print();
				
		//rides.writeAsCsv("data/test_window.csv");
		
		// execute the transformation pipeline
		env.execute("Popular Bike Stations from Kafka");
	}
	
	/**
	 * Assigns timestamps to TaxiRide records.
	 * Watermarks are a fixed time interval behind the max timestamp and are periodically emitted.
	 */
	public static class BikeRideTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<BikeRide> {

		public BikeRideTSExtractor() {
			super(Time.seconds(MAX_EVENT_DELAY));
		}

		@Override
		public long extractTimestamp(BikeRide ride) {
			return ride.startTime.getMillis();
		}
	}
	
	/**
	 * Maps taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class StationFlatMap implements MapFunction<BikeRide, Tuple2<String, Boolean>> {

		@Override
		public Tuple2<String, Boolean> map(BikeRide bikeRide) throws Exception {
			return new Tuple2<>(
					bikeRide.startStationName,
					true
			);
		}
	}

	/**
	 * Counts the number of rides arriving or departing.
	 */
	public static class RideCounter implements WindowFunction<
			Tuple2<String, Boolean>,                // input type
			Tuple4<String, Long, Boolean, Integer>, // output type
			Tuple,                                   // key type
			TimeWindow>                              // window type
	{

		@SuppressWarnings("unchecked")
		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple2<String, Boolean>> gridCells,
				Collector<Tuple4<String, Long, Boolean, Integer>> out) throws Exception {

			String stationName = ((Tuple2<String, Boolean>)key).f0;
			boolean isStart = ((Tuple2<Integer, Boolean>)key).f1;
			long windowTime = window.getEnd();

			int cnt = 0;
			for(Tuple2<String, Boolean> c : gridCells) {
				cnt += 1;
			}

			out.collect(new Tuple4<>(stationName, windowTime, isStart, cnt));
		}
	}

	/**
	 * Maps the grid cell id back to longitude and latitude coordinates.
	 */
	/*public static class GridToCoordinates implements
			MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {

		@Override
		public Tuple5<Float, Float, Long, Boolean, Integer> map(
				Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {

			return new Tuple5<>(
					GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0),
					cellCount.f1,
					cellCount.f2,
					cellCount.f3);
		}
	}*/

}
