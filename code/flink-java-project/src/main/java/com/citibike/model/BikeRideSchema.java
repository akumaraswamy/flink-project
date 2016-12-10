package com.citibike.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class BikeRideSchema implements DeserializationSchema<BikeRide>, SerializationSchema<BikeRide>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public byte[] serialize(BikeRide element) {
		return element.toString().getBytes();
	}

	@Override
	public BikeRide deserialize(byte[] message) {
		return BikeRide.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(BikeRide nextElement) {
		return false;
	}

	@Override
	public TypeInformation<BikeRide> getProducedType() {
		return TypeExtractor.getForClass(BikeRide.class);
	}
}
