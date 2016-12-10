package com.citibike.model;

import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 
 * @author akumara
 *
 */
public class BikeRide {

	private static transient DateTimeFormatter timeFormatter =
			DateTimeFormat.forPattern("M/dd/yyyy HH:mm:ss").withLocale(Locale.US).withZoneUTC();
	
	public long tripDuration;
	public DateTime startTime;
	public DateTime endTime;
	public long startStationId;
	public String startStationName;
	public float startStationLat;
	public float startStationLon;
	public long endStationId;
	public String endStationName;
	public float endStationLat;
	public float endStationLon;
	public long bikeId;
	public String userType;
	public int yearOfBirth;
	public String gender;
	
	public BikeRide(){}
	
	public BikeRide(long tripDuration, DateTime startTime, DateTime endTime, long startStationId,
					String startStationName, float startStationLat, float startStationLon,
					long endStationId, String endStationName, float endStationLat, float endStationLon,
					long bikeId, String userType, int yearOfBirth, String gender){
		
		this.tripDuration = tripDuration;
		this.startTime = startTime;
		this.endTime = endTime;
		this.startStationId = startStationId;
		this.startStationName = startStationName;
		this.startStationLat = startStationLat;
		this.startStationLon = startStationLon;
		this.endStationId = endStationId;
		this.endStationName = endStationName;
		this.endStationLat = endStationLat;
		this.endStationLon = endStationLon;
		this.bikeId = bikeId;
		this.userType = userType;
		this.yearOfBirth = yearOfBirth;
		this.gender = gender;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(tripDuration).append(",");
		sb.append(startTime.toString(timeFormatter)).append(",");
		sb.append(endTime.toString(timeFormatter)).append(",");
		sb.append(startStationId).append(",");
		sb.append(startStationName).append(",");
		sb.append(startStationLat).append(",");
		sb.append(startStationLon).append(",");
		sb.append(endStationId).append(",");
		sb.append(endStationName).append(",");
		sb.append(endStationLat).append(",");
		sb.append(endStationLon).append(",");
		sb.append(bikeId).append(",");
		sb.append(userType).append(",");
		sb.append(yearOfBirth).append(",");
		sb.append(gender);
		
		return sb.toString();
	}
	
	public static BikeRide fromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length <14) {
			System.out.println(tokens.length);
			throw new RuntimeException("Invalid record: " + line);
		}
		
		BikeRide ride = new BikeRide();
		String tripDuration = tokens[0].startsWith("\"")? tokens[0].substring(1,tokens[0].length()-1):tokens[0];
		ride.tripDuration = Long.parseLong(tripDuration);
		
		String startTime = tokens[1].startsWith("\"")? tokens[1].substring(1,tokens[1].length()-1):tokens[1];
		ride.startTime = DateTime.parse(startTime, timeFormatter);
		
		String endTime = tokens[2].startsWith("\"")? tokens[2].substring(1,tokens[2].length()-1):tokens[2];
		ride.endTime = DateTime.parse(endTime, timeFormatter);
		
		String stationId =  tokens[3].startsWith("\"")? tokens[3].substring(1,tokens[3].length()-1):tokens[3];
		ride.startStationId = Long.parseLong(stationId);
		
		ride.startStationName =  tokens[4].startsWith("\"")? tokens[4].substring(1,tokens[4].length()-1):tokens[4];
		
		String startStatLat = tokens[5].startsWith("\"")? tokens[5].substring(1,tokens[5].length()-1):tokens[5];
		ride.startStationLat = Float.parseFloat(startStatLat);
		
		String startStatLon = tokens[6].startsWith("\"")? tokens[6].substring(1,tokens[6].length()-1):tokens[6];
		ride.startStationLon = Float.parseFloat(startStatLon);
		
		String endStatId = tokens[7].startsWith("\"")? tokens[7].substring(1,tokens[7].length()-1):tokens[7];
		ride.endStationId = Long.parseLong(endStatId);
		
		
		ride.endStationName = tokens[8].startsWith("\"")? tokens[8].substring(1,tokens[8].length()-1):tokens[8];
		
		String endStatLat = tokens[9].startsWith("\"")? tokens[9].substring(1,tokens[9].length()-1):tokens[9];
		ride.endStationLat = Float.parseFloat(endStatLat);
		
		String endStatLon = tokens[10].startsWith("\"")? tokens[10].substring(1,tokens[10].length()-1):tokens[10];
		ride.endStationLon = Float.parseFloat(endStatLon);
		
		
		String bikeId = tokens[11].startsWith("\"")? tokens[11].substring(1,tokens[11].length()-1):tokens[11];
		ride.bikeId = Long.parseLong(bikeId);
		
		
		ride.userType = tokens[12].startsWith("\"")? tokens[12].substring(1,tokens[12].length()-1):tokens[12];
		String birthYear = tokens[13].startsWith("\"")? tokens[13].substring(1,tokens[13].length()-1):tokens[13];
		ride.yearOfBirth = birthYear.length() <= 0?0:Integer.parseInt(birthYear);
		
		ride.gender = tokens[14].startsWith("\"")? tokens[14].substring(1,tokens[14].length()-1):tokens[14];
		
		
		return ride;
	}

	
	@Override
	public boolean equals(Object other) {
		return other instanceof BikeRide &&
				this.tripDuration == ((BikeRide) other).tripDuration;
	}

	@Override
	public int hashCode() {
		return (int)this.tripDuration;
	}
	
	public static void main(String[] args){
		String line = "\"975\",\"9/1/2016 00:00:02\",\"9/1/2016 00:16:18\",\"312\",\"Allen St & Stanton St\",\"40.722055\",\"-73.989111\",\"313\",\"Washington Ave & Park Ave\",\"40.69610226\",\"-73.96751037\",\"22609\",\"Subscriber\",\"1985\",\"\"";
		BikeRide ride = BikeRide.fromString(line);
		System.out.println(ride);
		
		System.out.println(ride.startTime + " "+ride.endTime);
		
		Period period = new Period(ride.startTime,ride.endTime);
		System.out.println ("Period in days : "+period.getDays());
		
	}
}
