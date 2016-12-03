package com.citibike.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class StationStatusSchema implements  SerializationSchema<StationStatus>,DeserializationSchema<StationStatus>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	//@Override
	public byte[] serialize(StationStatus element) {
		byte[] serializedElement = null;
		try{
			serializedElement =	StationStatus.toString(element).getBytes();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return serializedElement;
	}

	@Override
	public StationStatus deserialize(byte[] message) {
		StationStatus stats = null;
		
		try {
			stats = StationStatus.fromString(new String(message));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return stats;
	}

	@Override
	public boolean isEndOfStream(StationStatus nextElement) {
		return false;
	}

	@Override
	public TypeInformation<StationStatus> getProducedType() {
		return TypeExtractor.getForClass(StationStatus.class);
	}
}
