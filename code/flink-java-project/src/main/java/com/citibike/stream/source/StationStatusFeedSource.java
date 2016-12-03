/**
 * 
 */
package com.citibike.stream.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.citibike.model.StationStatusFeed;
import com.citibike.model.StationStatus;

/**
 * This class operates as the data source for the station feed stream
 *  * 
 * @author Aruna Kumaraswamy
 *
 */
public class StationStatusFeedSource implements SourceFunction<StationStatus> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String urlPath;
	private final int maxDelayMsecs;
	private final int watermarkDelayMSecs;
	private final int servingSpeed;

	private transient BufferedReader reader;
	private transient HttpURLConnection connection;

	// public StationFeedSource(){}

	public StationStatusFeedSource(String urlPath) {
		this(urlPath, 0, 1);

	}

	public StationStatusFeedSource(String urlPath, int maxEventDelaySecs,
			int servingSpeedFactor) {
		if (maxEventDelaySecs < 0) {
			throw new IllegalArgumentException(
					"Max event delay must be positive");
		}
		this.urlPath = urlPath;
		this.maxDelayMsecs = maxEventDelaySecs * 1000;
		this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000
				: maxDelayMsecs;
		this.servingSpeed = servingSpeedFactor;

		System.out.println("StationFeedSource " + urlPath);
	}

	@Override
	public void run(SourceContext<StationStatus> ctx) throws Exception {

		try {
			System.out.println("Run");
			this.prepareStreamConnectionForRead();
			this.generateStreamData(ctx);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			cancel();
		}

	}

	private void generateStreamData(SourceContext<StationStatus> sourceContext)
			throws Exception {

		long servingStartTime = Calendar.getInstance().getTimeInMillis();
		long dataStartTime = 0L;

		Random rand = new Random(7452);
		PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
				32, new Comparator<Tuple2<Long, Object>>() {
					@Override
					public int compare(Tuple2<Long, Object> o1,
							Tuple2<Long, Object> o2) {
						return o1.f0.compareTo(o2.f0);
					}
				});

		// read first ride and insert it into emit schedule
		String line;
		StationStatus stationStatus = null;
		while (reader.ready() && (line = reader.readLine()) != null) {
			// read line, all records are packed as array
			// stationStatus = StationStatus.fromString(line);
			StationStatusFeed liveFeed = new StationStatusFeed(line);
			if (liveFeed.stationBeanList.isEmpty())
				return;
			Iterator stationIter = liveFeed.stationBeanList.iterator();
			while (stationIter.hasNext()) {
				if (stationStatus == null) {
					stationStatus = (StationStatus) stationIter.next();
					//System.out.println(stationStatus.stationName);
					// extract starting timestamp
					dataStartTime = getEventTime(stationStatus);
					// get delayed time
					long delayedEventTime = dataStartTime
							+ getNormalDelayMsecs(rand);

					emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime,
							stationStatus));
					// schedule next watermark
					long watermarkTime = dataStartTime + watermarkDelayMSecs;
					Watermark nextWatermark = new Watermark(watermarkTime
							- maxDelayMsecs - 1);
					emitSchedule.add(new Tuple2<Long, Object>(watermarkTime,
							nextWatermark));
				} else {
					stationStatus = (StationStatus) stationIter.next();
					//System.out.println(stationStatus.stationName);

					// insert all events into schedule that might be emitted
					// next
					long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule
							.peek().f0 : -1;
					long stattionEventTime = stationStatus != null ? getEventTime(stationStatus)
							: -1;
					if (emitSchedule.isEmpty() || // and no ride in schedule OR
							stattionEventTime < curNextDelayedEventTime
									+ maxDelayMsecs) // not enough rides in
														// schedule
					{
						// insert event into emit schedule
						long delayedEventTime = stattionEventTime
								+ getNormalDelayMsecs(rand);
						emitSchedule.add(new Tuple2<Long, Object>(
								delayedEventTime, stationStatus));

						// read next ride
						stattionEventTime = getEventTime(stationStatus);

					}

					// emit schedule is updated, emit next element in schedule
					Tuple2<Long, Object> head = emitSchedule.poll();
					long delayedEventTime = head.f0;

					long now = Calendar.getInstance().getTimeInMillis();
					long servingTime = toServingTime(servingStartTime,
							dataStartTime, delayedEventTime);
					long waitTime = servingTime - now;

					Thread.sleep((waitTime > 0) ? waitTime : 0);

					if (head.f1 instanceof StationStatus) {
						StationStatus emitStation = (StationStatus) head.f1;
						// emit ride
						sourceContext.collectWithTimestamp(emitStation,
								getEventTime(emitStation));
					} else if (head.f1 instanceof Watermark) {
						Watermark emitWatermark = (Watermark) head.f1;
						// emit watermark
						sourceContext.emitWatermark(emitWatermark);
						// schedule next watermark
						long watermarkTime = delayedEventTime
								+ watermarkDelayMSecs;
						Watermark nextWatermark = new Watermark(watermarkTime
								- maxDelayMsecs - 1);
						emitSchedule.add(new Tuple2<Long, Object>(
								watermarkTime, nextWatermark));
					}
				}// else

			} // while

		} // if
	}

	public long toServingTime(long servingStartTime, long dataStartTime,
			long eventTime) {
		long dataDiff = eventTime - dataStartTime;
		return servingStartTime + (dataDiff / this.servingSpeed);
	}

	public long getEventTime(StationStatus stationStatus) {
		return getJodaTime(stationStatus.getLastCommunicationTime()).getMillis();
	}

	private DateTime getJodaTime(String time){
		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss a");
		DateTime dt = formatter.parseDateTime(time);
		return dt;
	}
	
	public long getNormalDelayMsecs(Random rand) {
		long delay = -1;
		long x = maxDelayMsecs / 2;
		while (delay < 0 || delay > maxDelayMsecs) {
			delay = (long) (rand.nextGaussian() * x) + x;
		}
		return delay;
	}

	private void prepareStreamConnectionForRead() throws Exception {
		System.out.println("prepareStreamConnectionForRead");
		URL u = new URL(this.urlPath);
		connection = (HttpURLConnection) u.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Content-length", "0");
		connection.setUseCaches(false);
		connection.setAllowUserInteraction(false);
		connection.setConnectTimeout(1000);
		connection.setReadTimeout(3000);
		connection.connect();
		int status = connection.getResponseCode();

		switch (status) {
		case 200:
		case 201:
			this.reader = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));

		}
		return;

	}

	@Override
	public void cancel() {
		try {
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.connection != null) {
				this.connection.disconnect();
			}
		} catch (IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.connection = null;
		}
	}

}
