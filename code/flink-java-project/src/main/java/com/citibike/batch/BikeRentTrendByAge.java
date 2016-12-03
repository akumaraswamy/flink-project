package com.citibike.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.sinks.CsvTableSink;
import org.apache.flink.api.table.sinks.TableSink;

/**
 * BikeRentTrendByAge is a Flink batch program that takes CSV input.
 * It creates a CSV output.
 * 
 * It counts the trip based on age groups.
 * 
 * @author Aruna Kumaraswamy
 *
 */
public class BikeRentTrendByAge {

	public static void main(String[] args) throws Exception {
		
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		String output = params.getRequired("output");
		
		if (!input.endsWith("tripdata.csv")){
			throw new Exception("Unknown Format: expected bike ride trip data as csv");
			
		}
		//String input = "data/201609-citibike-tripdata.csv"; //201609-citibike-
		boolean[] fields = {false,false,false,false,false,false,false
				,false,false,false,false,false,false,true,false};
		
		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// Read the start time and gender
		DataSet<Tuple1< String>> trips =env.readCsvFile(input)
										.ignoreFirstLine()
										.includeFields(fields)
										.types(String.class);
									
		//Discard the timestamp
		DataSet<Tuple1<String>> ageGroupDS = trips
						.map(new AgeGroupExtractor());						
	
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		
		Table ageGroupAgg = tEnv
				.fromDataSet(ageGroupDS,"agegroup")
				.filter("agegroup != 'invalid'")
				.groupBy("agegroup").select("agegroup, agegroup.count as cnt");
		
	
		TableSink sink = new CsvTableSink(output,",");
		ageGroupAgg.writeToSink(sink);
		
		//finalResult.print();
		
		env.execute("BikeRentByAge");
		
	}
	
	public static class AgeGroupExtractor implements MapFunction<Tuple1<String>, Tuple1<String>> {

		@Override
		public Tuple1<String> map(Tuple1<String> ageGroup) throws Exception {

			String yearOfBirthStr = ageGroup.f0.substring(1,ageGroup.f0.length()-1);
			
			int yearOfBirth = -1;
			if (!(yearOfBirthStr.trim().isEmpty()))
				yearOfBirth = Integer.parseInt(yearOfBirthStr);
			String ageGroupStr = "invalid";
			if (yearOfBirth >= 1945 && yearOfBirth <=1954 ){
				ageGroupStr = "Boomers I";
			}else if (yearOfBirth >= 1955 && yearOfBirth <=1965 ){
				ageGroupStr = "Boomers II";
			}else if (yearOfBirth >= 1966 && yearOfBirth <=1976 ){
				ageGroupStr = "Gen X";
			}else if (yearOfBirth >= 1977 && yearOfBirth <=1994 ){
				ageGroupStr = "Gen Y";
			}if (yearOfBirth >= 1995 ){
				ageGroupStr = "Gen Z";
			}
			return new Tuple1<>(ageGroupStr);
		}
	}
}
