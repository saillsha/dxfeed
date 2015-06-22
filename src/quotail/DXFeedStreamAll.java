package quotail;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

// dxfeed imports
import com.devexperts.util.TimeFormat;
import com.dxfeed.api.*;
import com.dxfeed.event.EventType;
import com.dxfeed.event.TimeSeriesEvent;
import com.dxfeed.event.market.TimeAndSale;
import com.dxfeed.api.osub.WildcardSymbol;
import com.devexperts.qd.qtp.QDEndpoint;


//kafka imports
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class DXFeedStreamAll{
	static Producer<byte[], byte[]> producer;
	static int counter;
	static Mode mode;
	public enum Mode{
		REALTIME, TIMESERIES, FILE
	}
	
	public static void main(String[] args){
		// config block for kafka producer
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("partitioner.class", "quotail.TickerPartitioner");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<byte[], byte[]>(config);
		if(args.length == 1){
			new DXFeedStreamAll(Mode.FILE, args[0], 0);
		}
		else if(args.length == 2){
			new DXFeedStreamAll(Mode.TIMESERIES, args[0], Long.parseLong(args[1]));
		}
		else{
			new DXFeedStreamAll(Mode.REALTIME, null, 0);
		}
	}
	
	public DXFeedStreamAll(Mode mode, String info, long fromTime){
		DXFeed feed;
		switch(mode){
		case TIMESERIES:
			feed = DXFeed.getInstance();
			// replay events using time series subscription
			DXFeedTimeSeriesSubscription<TimeAndSale> timeSeriesSub = feed.createTimeSeriesSubscription(TimeAndSale.class);
			timeSeriesSub.addEventListener(new TradeListener());
			timeSeriesSub.setFromTime(1430870078205L);
			timeSeriesSub.addSymbols(info);
			break;
		case REALTIME:
			feed = DXFeed.getInstance();
			DXFeedSubscription<TimeAndSale> sub = feed.createSubscription(TimeAndSale.class);
			sub.addEventListener(new TradeListener());
			sub.addSymbols(WildcardSymbol.ALL);
			break;
		case FILE:
			try{
				TradeListener listener = new TradeListener();
				BufferedReader reader = new BufferedReader(new FileReader(info));
				String line;
				while((line = reader.readLine()) != null){
					TimeAndSale t = DXFeedUtils.parseTrade(line);
					if(t != null){
						List<TimeAndSale> tns = new ArrayList<TimeAndSale>();
						tns.add(t);
						listener.processTrades(tns);
					}
				}
				reader.close();
			}
			catch(FileNotFoundException e){
				System.out.println("Could not find " + info);
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	public class TradeListener implements DXFeedEventListener<TimeAndSale>{
		public void processTrades(List<TimeAndSale> events){
			List<KeyedMessage<byte[], byte[]>> trades = new ArrayList<KeyedMessage<byte[], byte[]>>();
			for (TimeAndSale event : events){
				// this will be true until ~2018 (when there are 2020 LEAPs)
				String ticker = event.getEventSymbol().substring(1, event.getEventSymbol().indexOf('1'));
		        ByteArrayOutputStream b = new ByteArrayOutputStream();
				try{
			        ObjectOutputStream o = new ObjectOutputStream(b);
			        o.writeObject(event);
				}
				catch(IOException e){
					e.printStackTrace();
				}
				trades.add(new KeyedMessage<byte[], byte[]>("trades", ticker.getBytes(), b.toByteArray()));
				System.out.println(++counter + "\t" + event);
			}
			producer.send(trades);
		}
		public void eventsReceived(List<TimeAndSale> events) {
			processTrades(events);
		}
	}
}
