package com.citibike.stream;

import java.util.Properties;

import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.sinks.CsvTableSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import com.citibike.model.StationStatus;
import com.citibike.model.StationStatusSchema;

/**
 * This class is an example of how stream data can be archived into a sink
 * for trend analysis.
 * 
 * @author aruna
 *
 */
public class StationStatusDataArchive {

	private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	private static final String RIDE_SPEED_GROUP = "rideSpeedGroup";
	public static final String LIVE_STATION_TOPIC = "liveStationFeed3";
	
	private static final int MAX_EVENT_DELAY = 60;
	
	
	public static void main(String[] args) throws Exception {

		final int popThreshold = 20; // threshold for popular places
		ParameterTool params = ParameterTool.fromArgs(args);
		String output = params.getRequired("output");
		
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000);
		StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);
		
		// configure the Kafka consumer
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
		kafkaProps.setProperty("group.id", RIDE_SPEED_GROUP);
		// always read the Kafka topic from the start
		kafkaProps.setProperty("auto.offset.reset", "earliest");

		// The JSON field names and types
		/*String[] fieldNames =  new String[] { "stationName", "availableDocks", "latitude",
								"longitude","availableBikes","testStation","lastCommunicationTime",
								"landMark"
								};
		Class<?>[] fieldTypes = new Class<?>[] { String.class, Integer.class, Float.class,
								Float.class,Integer.class,Boolean.class,String.class,Boolean.class};
	
		KafkaJsonTableSource kafkaTableSource = new Kafka09JsonTableSource(
				LIVE_STATION_TOPIC,
				kafkaProps,
		    fieldNames,
		    fieldTypes);
		
		StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);
		tEnv.registerTableSource("station_status", kafkaTableSource);
		//StreamTableSink tSink = new StreamTableSink();
		
		Table station_status = tEnv.ingest("station_status");
	
		//Table filteredRecords = station_status.filter("testStation = false");
		CsvTableSink sink = new CsvTableSink(output,",");
		station_status.writeToSink(sink);*/
		
		// create a Kafka consumer
		FlinkKafkaConsumer09<StationStatus> consumer = new FlinkKafkaConsumer09<>(
						LIVE_STATION_TOPIC,
						new StationStatusSchema(),
						//new JsonRowDeserializationSchema(fieldNames, fieldTypes),
						kafkaProps);
		
		DataStreamSource<StationStatus> dataRows = env.addSource(consumer);
		
		dataRows.print();
		CsvTableSink sink = new CsvTableSink(output,",");
		//dataRows.writeAsCsv(output);
		
		//tEnv.registerDataStream("stationstatus", dataRows);
		//Table station_status = tEnv.ingest("stationstatus");
		//tEnv.registerTable("station_status", station_status);
		//Table filteredRecords = station_status.filter("testStation = false");
		//station_status.writeToSink(sink);
		
		env.execute("Example of archive data stream");
	}
	
	
	
}
