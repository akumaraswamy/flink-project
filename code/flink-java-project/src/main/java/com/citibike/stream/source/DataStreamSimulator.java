package com.citibike.stream.source;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.citibike.model.BikeRide;


public class DataStreamSimulator implements SourceFunction<BikeRide> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final int maxDelayMsecs;
	private final int watermarkDelayMSecs;

	private final String dataFilePath;
	private final int servingSpeed;

	private transient BufferedReader reader;
	private transient ZipInputStream zipStream;

	/**
	 * Serves the BikeRide records from the specified and ordered zipped input file.
	 * Rides are served exactly in order of their time stamps
	 * at the speed at which they were originally generated.
	 *
	 * @param dataFilePath The zipped input file from which the BikeRide records are read.
	 */
	public DataStreamSimulator(String dataFilePath) {
		this(dataFilePath, 0, 1);
	}

	/**
	 * Serves the BikeRide records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The zipped input file from which the BikeRide records are read.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public DataStreamSimulator(String dataFilePath, int servingSpeedFactor) {
		this(dataFilePath, 0, servingSpeedFactor);
	}

	/**
	 * Serves the BikeRide records from the specified and ordered gzipped input file.
	 * Rides are served out-of time stamp order with specified maximum random delay
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the BikeRide records are read.
	 * @param maxEventDelaySecs The max time in seconds by which events are delayed.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public DataStreamSimulator(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
		if(maxEventDelaySecs < 0) {
			throw new IllegalArgumentException("Max event delay must be positive");
		}
		this.dataFilePath = dataFilePath;
		this.maxDelayMsecs = maxEventDelaySecs * 1000;
		this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
		this.servingSpeed = servingSpeedFactor;
	}

	@Override
	public void run(SourceContext<BikeRide> sourceContext) throws Exception {
		//System.out.println("Running the source");
		
		zipStream = new ZipInputStream(new FileInputStream(dataFilePath));
		ZipEntry entry;
        while((entry = zipStream.getNextEntry())!=null)
        {
        	
        	reader = new BufferedReader(new InputStreamReader(zipStream, "UTF-8"));
        	generateUnorderedStream(sourceContext);
        	this.reader.close();
        	this.reader = null;
        }
		this.zipStream.close();
		this.zipStream = null;
		//System.out.println("Run completed.");

	}

	private void generateUnorderedStream(SourceContext<BikeRide> sourceContext) throws Exception {

		long servingStartTime = Calendar.getInstance().getTimeInMillis();
		long dataStartTime;

		Random rand = new Random(7452);
		PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
				32,
				new Comparator<Tuple2<Long, Object>>() {
					@Override
					public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
						return o1.f0.compareTo(o2.f0);
					}
				});

		// read first ride and insert it into emit schedule
		String line;
		BikeRide ride;
		if (reader.ready() && (line = reader.readLine()) != null) {
			
			if (line.indexOf("tripduration") != -1){//header
				line = reader.readLine();
			}
			// read first ride
			ride = BikeRide.fromString(line);
			// extract starting timestamp
			dataStartTime = ride.startTime.getMillis();
			// get delayed time
			long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

			emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));
			// schedule next watermark
			long watermarkTime = dataStartTime + watermarkDelayMSecs;
			Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
			emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));

		} else {
			return;
		}

		// peek at next ride
		if (reader.ready() && (line = reader.readLine()) != null) {
				ride = BikeRide.fromString(line);
		}

		// read rides one-by-one and emit a random ride from the buffer each time
		while (emitSchedule.size() > 0 || reader.ready()) {

			// insert all events into schedule that might be emitted next
			long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
			long rideEventTime = ride != null ? ride.endTime.getMillis() : -1;
			while(
					ride != null && ( // while there is a ride AND
						emitSchedule.isEmpty() || // and no ride in schedule OR
						rideEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough rides in schedule
					)
			{
				// insert event into emit schedule
				long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
				emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

				// read next ride
				if (reader.ready() && (line = reader.readLine()) != null) {
					//System.out.println("Processing "+line);
					ride = BikeRide.fromString(line);
					rideEventTime = ride.startTime.getMillis(); //getEventTime(ride);
				}
				else {
					ride = null;
					rideEventTime = -1;
				}
			}

			// emit schedule is updated, emit next element in schedule
			Tuple2<Long, Object> head = emitSchedule.poll();
			long delayedEventTime = head.f0;

			long now = Calendar.getInstance().getTimeInMillis();
			long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
			long waitTime = servingTime - now;

			Thread.sleep( (waitTime > 0) ? waitTime : 0);

			if(head.f1 instanceof BikeRide) {
				BikeRide emitRide = (BikeRide)head.f1;
				// emit ride
				sourceContext.collectWithTimestamp(emitRide, emitRide.startTime.getMillis()); //getEventTime(emitRide)
			}
			else if(head.f1 instanceof Watermark) {
				Watermark emitWatermark = (Watermark)head.f1;
				// emit watermark
				sourceContext.emitWatermark(emitWatermark);
				// schedule next watermark
				long watermarkTime = delayedEventTime + watermarkDelayMSecs;
				Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
				emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
			}
		}
	}

	public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
		long dataDiff = eventTime - dataStartTime;
		return servingStartTime + (dataDiff / this.servingSpeed);
	}

	/*public long getEventTime(BikeRide ride) {
		if (ride.isStart) {
			return ride.startTime.getMillis();
		}
		else {
			return ride.endTime.getMillis();
		}
	}
*/
	public long getNormalDelayMsecs(Random rand) {
		long delay = -1;
		long x = maxDelayMsecs / 2;
		while(delay < 0 || delay > maxDelayMsecs) {
			delay = (long)(rand.nextGaussian() * x) + x;
		}
		return delay;
	}

	@Override
	public void cancel() {
		try {
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.zipStream != null) {
				this.zipStream.close();
			}
		} catch(IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.zipStream = null;
		}
	}
}
