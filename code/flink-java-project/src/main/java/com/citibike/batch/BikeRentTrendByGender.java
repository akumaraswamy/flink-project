package com.citibike.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.sinks.CsvTableSink;
import org.apache.flink.api.table.sinks.TableSink;

/**
 * BikeRentTrendByGender is a Flink batch program that takes CSV input.
 * It creates a CSV output.
 * 
 * It counts the trip based on gender.
 * 
 * @author Aruna Kumaraswamy
 *
 */
public class BikeRentTrendByGender {
	
	public static void main(String[] args) throws Exception {
		
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		String output = params.getRequired("output");
		
		if (!input.endsWith("tripdata.csv")){
			throw new Exception("Unknown Format: expected bike ride trip data as csv");
			
		}
		boolean[] fields = {false,true,false,false,false,false,false
				,false,false,false,false,false,false,false,true};
		
		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// Read the start time and gender
		DataSet<Tuple2< String, String>> trips =env.readCsvFile(input)
										.ignoreFirstLine()
										.includeFields(fields)
										.types(String.class,String.class);
									
		//Discard the timestamp
		DataSet<Tuple2<String, String>> tripDateFixedDS = trips
						.map(new TripDateExtractor());						
		
		//tripDateFixedDS.print();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		
		Table tripsByDate = tEnv
				.fromDataSet(tripDateFixedDS,"tripdate,gender")
				.filter("gender != 'unknown'")
				.groupBy("tripdate,gender").select("tripdate, gender, gender.count as cnt");
		
		Table tripsByDateFemale = tripsByDate
									.filter("gender = 'Female'")
									.select("tripdate as tdf, cnt as female");
		tEnv.registerTable("trip_female", tripsByDateFemale);
		Table tripsByDateMale = tripsByDate
				.filter("gender = 'Male'")
				.select("tripdate as tdm, cnt as male");
		tEnv.registerTable("trip_male", tripsByDateMale);
		Table flattenedTripByGender = tEnv.sql("select tdm, male, female from trip_male, trip_female where tdf = tdm");

		DataSet<Row> finalResult = tEnv.toDataSet(flattenedTripByGender, Row.class);
		
		//finalResult.print();
		TableSink sink = new CsvTableSink(output,",");
		flattenedTripByGender.writeToSink(sink);
		
		
		env.execute("TripTrendByGender");
		
	}
	
	public static class TripDateExtractor implements MapFunction<Tuple2<String,String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(Tuple2<String, String> trip) throws Exception {

			// Extract the date, disregard time
			String tripDate = trip.f0.substring(1, trip.f0.indexOf(" "));
			// Set gender
			String gender = "unknown";
			switch(trip.f1){
				case "\"1\"": gender = "Male"; break;
				case "\"2\"": gender = "Female"; break;
			}
			
			return new Tuple2<>(tripDate, gender);
		}
	}

	
}
