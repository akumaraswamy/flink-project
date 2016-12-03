package com.citibike.model;

import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.citibike.utils.JSONMarshaller;

/**
 * This class models the station status live data feed.
 * 
 * @author Aruna Kumaraswamy
 *
 */
public class StationStatus {
	private int id;
	private String stationName;
	private int availableDocks;
	private int totalDocks;
	private float latitude;
	private float longitude;
	private String statusValue;
	private int statusKey;
	private int availableBikes;
	private String stAddress1;
	private String stAddress2;
	private String city;
	private String location;
	private String postalCode;
	private String altitude;
	private boolean testStation;
	private String lastCommunicationTime;
	private boolean landMark;
	
	private static transient DateTimeFormatter timeFormatter =
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
	
	
	public StationStatus(){}
	
	public StationStatus(int id, String stationName,int availableDocks,	int totalDocks,
			float latitude, float longitude, String statusValue, int statusKey,
			int availableBikes, String stAddress1, String stAddress2, String city,
			String location, String postalCode, String altitude, boolean testStation,
			String lastCommunicationTime,  boolean landMark){
			this.id = id;
			this.stationName = stationName;
			this.availableDocks = availableDocks;
			this.totalDocks = totalDocks;
			this.latitude = latitude;
			this.longitude = longitude;
			this.statusValue = statusValue;
			this.statusKey = statusKey;
			this.availableBikes = availableBikes;
			this.stAddress1 = stAddress1;
			this.stAddress2 = stAddress2;
			this.city = city;
			this.location = location;
			this.postalCode = postalCode;
			this.altitude = altitude;
			this.testStation = testStation;
			this.lastCommunicationTime = lastCommunicationTime;
			this.landMark = landMark;
		
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.id).append(",");
		sb.append(this.stationName).append(",");
		sb.append(this.availableDocks).append(",");
		sb.append(this.totalDocks).append(",");
		sb.append(this.latitude).append(",");
		sb.append(this.longitude).append(",");
		sb.append(this.statusValue).append(",");
		sb.append(this.statusKey).append(",");
		sb.append(this.availableBikes).append(",");
		sb.append(this.stAddress1).append(",");
		sb.append(this.stAddress2).append(",");
		sb.append(this.city).append(",");
		sb.append(this.location).append(",");
		sb.append(this.postalCode).append(",");
		sb.append(this.altitude).append(",");
		sb.append(this.testStation).append(",");
		sb.append(this.lastCommunicationTime).append(",");
		sb.append(this.landMark);
		
		return sb.toString();
		
	}
	
	
	@Override
	public boolean equals(Object other) {
		return (other instanceof StationStatus
				&& this.toString().equals(other.toString()));
	}
		
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
	
	public static StationStatus fromString(String line) throws Exception{
		System.out.println(line);
		JSONMarshaller<StationStatus> marshaller = 
				new JSONMarshaller<>();
		StationStatus stationStatus = marshaller.fromJSON(line, StationStatus.class);
		return stationStatus;
		
	}
	
	public static String toString(StationStatus status )throws Exception{
		StringBuffer strStatus =new StringBuffer();
		/*JSONMarshaller<StationStatus> marshaller = 
				new JSONMarshaller<>();
		strStatus = marshaller.toJSON(status);*/
		
		if (status != null){
			strStatus.append(status.getId()).append(",");
			strStatus.append(status.getStationName()).append(",");
			strStatus.append(status.getAvailableBikes()).append(",");
			strStatus.append(status.getAvailableDocks()).append(",");
			strStatus.append(status.isLandMark()).append(",");
			strStatus.append(status.isTestStation());
		}
		return strStatus.toString();
		
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getStationName() {
		return stationName;
	}

	public void setStationName(String stationName) {
		this.stationName = stationName;
	}

	public int getAvailableDocks() {
		return availableDocks;
	}

	public void setAvailableDocks(int availableDocks) {
		this.availableDocks = availableDocks;
	}

	public int getTotalDocks() {
		return totalDocks;
	}

	public void setTotalDocks(int totalDocks) {
		this.totalDocks = totalDocks;
	}

	public float getLatitude() {
		return latitude;
	}

	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}

	public float getLongitude() {
		return longitude;
	}

	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}

	public String getStatusValue() {
		return statusValue;
	}

	public void setStatusValue(String statusValue) {
		this.statusValue = statusValue;
	}

	public int getStatusKey() {
		return statusKey;
	}

	public void setStatusKey(int statusKey) {
		this.statusKey = statusKey;
	}

	public int getAvailableBikes() {
		return availableBikes;
	}

	public void setAvailableBikes(int availableBikes) {
		this.availableBikes = availableBikes;
	}

	public String getStAddress1() {
		return stAddress1;
	}

	public void setStAddress1(String stAddress1) {
		this.stAddress1 = stAddress1;
	}

	public String getStAddress2() {
		return stAddress2;
	}

	public void setStAddress2(String stAddress2) {
		this.stAddress2 = stAddress2;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}

	public String getAltitude() {
		return altitude;
	}

	public void setAltitude(String altitude) {
		this.altitude = altitude;
	}

	public boolean isTestStation() {
		return testStation;
	}

	public void setTestStation(boolean testStation) {
		this.testStation = testStation;
	}

	public String getLastCommunicationTime() {
		return lastCommunicationTime;
	}

	public void setLastCommunicationTime(String lastCommunicationTime) {
		this.lastCommunicationTime = lastCommunicationTime;
	}

	public boolean isLandMark() {
		return landMark;
	}

	public void setLandMark(boolean landMark) {
		this.landMark = landMark;
	}
}
