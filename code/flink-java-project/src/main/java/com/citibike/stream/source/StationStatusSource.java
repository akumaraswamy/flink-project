/**
 * 
 */
package com.citibike.stream.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Random;

import javax.net.ssl.HttpsURLConnection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.citibike.model.StationStatus;
import com.citibike.model.StationStatusFeed;

/**
 * @author aruna
 *
 */
public class StationStatusSource implements SourceFunction<String> {
	
	public String urlPath;
	private final int maxDelayMsecs;
	private final int watermarkDelayMSecs;
	private final int servingSpeed;
	
	private static transient DateTimeFormatter timeFormatter =
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
	
	public StationStatusSource(String urlPath) {
		this(urlPath, 0, 1);

	}

	public StationStatusSource(String urlPath, int maxEventDelaySecs,
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
	public void run(
			org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<String> ctx)
			throws Exception {
		HttpsURLConnection httpConn = null;
		String line = null;
		DateTime currentTime = new DateTime();
		DateTime endTime = currentTime.plusMinutes(2); //Stream for 2 minutes , in 10 second interval
		
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
		
		
		
		do {
			try {
				URL url = new URL(urlPath);
				URLConnection urlConn = url.openConnection();
				if (!(urlConn instanceof HttpsURLConnection)) {
					throw new IOException("URL is not an Https URL");
				}
				httpConn = (HttpsURLConnection) urlConn;
				httpConn.setAllowUserInteraction(false);
				httpConn.setInstanceFollowRedirects(true);
				httpConn.setRequestMethod("GET");
				httpConn.setReadTimeout(50 * 1000);

				BufferedReader is = new BufferedReader(new InputStreamReader(
						httpConn.getInputStream()));

				while ((line = is.readLine()) != null) {
					System.out.println("**********************************");
					System.out.println(line);
					System.out.println("**********************************");
					StationStatusFeed feed = new StationStatusFeed(line);
					//line.substring(line.indexOf("["), line.indexOf("]"));
					DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss a");
					DateTime dt = formatter.parseDateTime(feed.executionTime);
					dataStartTime = dt.getMillis();
					// get delayed time
					long delayedEventTime = dataStartTime
							+ getNormalDelayMsecs(rand);

					emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime,
							feed.toJSON()));
				}
				// emit schedule is updated, emit next element in schedule
				Tuple2<Long, Object> head = emitSchedule.poll();
				long delayedEventTime = head.f0;

				long now = Calendar.getInstance().getTimeInMillis();
				long servingTime = toServingTime(servingStartTime,
						dataStartTime, delayedEventTime);
				long waitTime = servingTime - now;

				Thread.sleep((waitTime > 0) ? waitTime : 0);

				if (head.f1 instanceof String) {
					String emitStation = (String) head.f1;
					// emit ride
					ctx.collectWithTimestamp(emitStation,
							dataStartTime);
				} else if (head.f1 instanceof Watermark) {
					Watermark emitWatermark = (Watermark) head.f1;
					// emit watermark
					ctx.emitWatermark(emitWatermark);
					// schedule next watermark
					long watermarkTime = delayedEventTime
							+ watermarkDelayMSecs;
					Watermark nextWatermark = new Watermark(watermarkTime
							- maxDelayMsecs - 1);
					emitSchedule.add(new Tuple2<Long, Object>(
							watermarkTime, nextWatermark));
				}
				
				
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (SocketTimeoutException e) {
				e.printStackTrace();
			} catch (IOException e) {

				e.printStackTrace();
			} finally {
				httpConn.disconnect();
			}
			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} while (!(endTime.isAfterNow()));

		
		
	}

	public long toServingTime(long servingStartTime, long dataStartTime,
			long eventTime) {
		long dataDiff = eventTime - dataStartTime;
		return servingStartTime + (dataDiff / this.servingSpeed);
	}

	public long getNormalDelayMsecs(Random rand) {
		long delay = -1;
		long x = maxDelayMsecs / 2;
		while (delay < 0 || delay > maxDelayMsecs) {
			delay = (long) (rand.nextGaussian() * x) + x;
		}
		return delay;
	}
	
	@Override
	public void cancel() {
		return;
		
	}
}
