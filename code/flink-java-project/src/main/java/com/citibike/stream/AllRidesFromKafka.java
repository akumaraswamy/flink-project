package com.citibike.stream;

import java.util.Properties;

import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.sinks.CsvTableSink;
import org.apache.flink.api.table.sinks.TableSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import com.citibike.model.BikeRide;
import com.citibike.model.BikeRideSchema;

/**
 * 
 * @author Aruna Kumaraswamy
 *
 */
public class AllRidesFromKafka {
	
	private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	private static final String RIDE_SPEED_GROUP = "rideSpeedGroup";
	public static final String RIDES_TABLE_SINK_TOPIC = "ridesToTableSink";
	
	//private static final int MAX_EVENT_DELAY = 60;
	
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String output = params.getRequired("output");
		
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
				RIDES_TABLE_SINK_TOPIC,
				new BikeRideSchema(),
				kafkaProps);
		
		// create a BikeRide data stream
		StreamTableEnvironment stableEnv = TableEnvironment.getTableEnvironment(env);
		DataStream<BikeRide> rides = env.addSource(consumer);
		Table bikeRideTable = stableEnv.fromDataStream(rides);
		//rides.print();
		TableSink sink = new CsvTableSink(output,",");
		bikeRideTable.writeToSink(sink);
		
		
		// execute the transformation pipeline
		env.execute("All rides from Kafka");
	}

}
