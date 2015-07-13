package quotail;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.BufferedWriter;

import com.dxfeed.event.market.Side;
import com.dxfeed.event.market.TimeAndSale;
import com.dxfeed.event.market.Summary;
import com.dxfeed.event.market.Trade;
import com.dxfeed.promise.Promise;
import com.dxfeed.api.DXFeed;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;

public class ConsumerWorkerThread implements Runnable {
    private final int CLUSTER_WAIT_TIME = 400;
    private final int CLUSTER_QUANTITY_THRESHOLD = 50;
    private final int CLUSTER_MONEY_THRESHOLD = 50000;
    private final long SUMMARY_TIMEOUT = 500;
	private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
    private static PrintWriter tradeOut = null;
    private static PrintWriter clusterOut = null;
    private static Producer<String, String> producer = null;
    public static DXFeed feed = DXFeed.getInstance();
    private static Timer timer;
    private static Calendar calendar = new GregorianCalendar();
    private static final long REDIS_KEY_EXPIRY_TIME;
    
    static{
    	// set expiry of redis keys to 9:00 of the following day
    	calendar.add(Calendar.DATE, 1);
		calendar.set(Calendar.HOUR, 9);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.AM_PM, 0);
    	REDIS_KEY_EXPIRY_TIME = calendar.getTimeInMillis();
    }

	private KafkaStream m_stream;
    private int m_threadNumber;

    private HashMap<String, Cluster> contractsMap;
    private Map<String, String> aggVolMap = new HashMap<String, String>();
    SpreadTracker spreadTracker = new SpreadTracker();
    
    public void configureKafkaProducer(){
    	Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		// we have the option of overriding the default serializer here, could this be used to remove the casting that the consumer has to do?
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
    }

    public ConsumerWorkerThread(KafkaStream a_stream, int a_threadNumber, String tradeFile, String clusterFile) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        timer = new Timer(true);
        contractsMap = new HashMap<String, Cluster>();
    	aggVolMap.put("CA", "0");
    	aggVolMap.put("CB", "0");
    	aggVolMap.put("CM", "0");
    	aggVolMap.put("PA", "0");
    	aggVolMap.put("PB", "0");
    	aggVolMap.put("PM", "0");
    	if(producer == null)
    		configureKafkaProducer();
        try{
        	if(tradeFile != null){
        		tradeOut = new PrintWriter(new BufferedWriter(new FileWriter(tradeFile, true)));
        	}
        	if(clusterFile != null){
        		clusterOut = new PrintWriter(new BufferedWriter(new FileWriter(clusterFile, true)));
        	}
        }
        catch(IOException e){
        	e.printStackTrace();
        }
    }
    
    private class ClusteringTask extends TimerTask{
    	private Cluster cluster;
    	private String symbol;
    	private String ticker;
    	private Bin bin;
    	public ClusteringTask(Cluster cluster){
    		this.cluster = cluster;
    		this.symbol = cluster.trades.getFirst().getEventSymbol();
    		this.ticker = DXFeedUtils.getTicker(symbol);
    	}
    	
    	public void addBin(Bin bin){ this.bin = bin; }
    	private long elapsedTime(){
    		return System.currentTimeMillis() - cluster.trades.get(0).getTime();
    	}
    	
    	public void run(){
    		try{
	    		synchronized(contractsMap){
					contractsMap.remove(symbol);
	    		}
				if(cluster.quantity >= CLUSTER_QUANTITY_THRESHOLD){
		    		
					cluster.classifyCluster();
					String denormalizedSymbol = DXFeedUtils.denormalizeContract(symbol);
		    		Promise<Summary> summaryPromise = feed.getLastEventPromise(Summary.class, denormalizedSymbol);
		    		if(summaryPromise.awaitWithoutException(SUMMARY_TIMEOUT, TimeUnit.MILLISECONDS)){
			    		cluster.openinterest = summaryPromise.getResult().getOpenInterest();
		    		}
		    		Promise<Trade> tradePromise = feed.getLastEventPromise(Trade.class, denormalizedSymbol);
		    		if(tradePromise.awaitWithoutException(SUMMARY_TIMEOUT, TimeUnit.MILLISECONDS)){
		    			cluster.volume = (int)tradePromise.getResult().getDayVolume();
		    		}
	    			System.out.println("CLUSTER FOUND (" + elapsedTime() + "ms) [" + cluster.toJSON() + "]");
		    		if(cluster.isSpreadLeg){
		    			boolean isSpreadProcessed;
		    			synchronized(bin){
			    			System.out.println(String.format("SPREAD CLUSTER FOUND (%d/%d)\t%s\t%d\t%f\t%f\t%f\t%d", bin.numProcessed, bin.legs.size(),
			    					cluster.trades.get(0).getEventSymbol(), cluster.trades.get(0).getTime(), cluster.trades.get(0).getBidPrice(),
			    					cluster.trades.get(0).getAskPrice(), cluster.trades.get(0).getPrice(), cluster.quantity));
		    				bin.incrProcessed();
		    				isSpreadProcessed = bin.isProcessed();
		    				if(isSpreadProcessed) spreadTracker.removeBin(bin, ticker);
		    			}
		    			if(isSpreadProcessed){
			    			System.out.println("Spread PROCESSED: (" + elapsedTime() + "ms)" + bin.toString());
		    				KeyedMessage<String, String> message = new KeyedMessage<String, String>("clusters", DXFeedUtils.getTicker(symbol), bin.toString());
		    				producer.send(message);
		    			}
		    			else{
		    				
		    			}
		    		}
		    		else{
	    				KeyedMessage<String, String> message = new KeyedMessage<String, String>("clusters", DXFeedUtils.getTicker(symbol), "[" + cluster.toJSON() + "]");
	    				producer.send(message);
		    		}
		    		if(clusterOut != null){
		    			// write out cluster to file
		    			clusterOut.println(cluster.toJSON());
			    		clusterOut.flush();
		    		}
	    		}
//				else if(cluster.isSpreadLeg){
//					synchronized(spreadsMap){
//						// reset the spread count for non-clusters
//						spreadsMap.get(DXFeedUtils.getTicker(symbol)).reset();
//					}
//				}
			}catch(NullPointerException e){
				e.printStackTrace();
			}catch(Exception e){
				e.printStackTrace();
			}
    	}
    }
    
    public void run() {
    	System.out.println("starting thread..." + m_threadNumber);
	    Jedis jedis = jedisPool.getResource();
    	ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
	    TimeAndSale t = null;
	    // contract and ticker symbols
	    String symbol, ticker;

	    try{
	    	while (it.hasNext()){
	        	byte[] serializedTrade = it.next().message();
	         	ByteArrayInputStream in = new ByteArrayInputStream(serializedTrade);

    	    	// convert byte[] to TimeAndSale object
    		    ObjectInputStream is = new ObjectInputStream(in);
    		    t = (TimeAndSale)is.readObject();
    		    t.setEventSymbol(DXFeedUtils.normalizeContract(t.getEventSymbol()));
//    		    System.out.println(t);
    		    symbol = t.getEventSymbol();
    		    ticker = DXFeedUtils.getTicker(symbol);
    		    // invalid trade if size 0, continue
    		    if(t.getSize() == 0) continue;
    		    if(tradeOut != null){
    		    	// print trade out to file if enabled in run-time options
    		    	tradeOut.println(DXFeedUtils.serializeTrade(t));
    		    }

    		    // update redis with aggregate counts
    		    char optionType = t.getEventSymbol().substring(7).lastIndexOf('C') == -1 ? 'P' : 'C';
		    	String hashKey = optionType + (t.getAggressorSide() == Side.BUY ? "A" : (t.getAggressorSide() == Side.SELL ? "B" : "M"));
		    	String redisKey = ticker + "_agg_vol";
		    	Map<String, String> updatedVol = new HashMap<String, String>();
		    	if(jedis.exists(redisKey)){
		    		String agg_vol = jedis.hmget(redisKey, hashKey).get(0);
		    		agg_vol = agg_vol == null ? "0" : agg_vol;
    		    	int aggVol = Integer.parseInt(agg_vol) + (int)t.getSize();
    		    	updatedVol.put(hashKey, "" + aggVol);
    		    	jedis.hmset(redisKey, updatedVol);
		    	}
    		    else{
    		    	updatedVol.putAll(aggVolMap);
    		    	updatedVol.put(hashKey, "" + t.getSize());
    		    	jedis.hmset(redisKey, updatedVol);
    		    	jedis.expireAt(redisKey, REDIS_KEY_EXPIRY_TIME);
    		    }

		    	synchronized(contractsMap){
    		    	if(!contractsMap.containsKey(symbol)){
    		    		// create new cluster for this trade if none exists yet for the contract
    		    		createCluster(t, symbol, ticker);
	    		    }
	    		    else{
	    		    	Cluster cluster = contractsMap.get(symbol);
	    		    	if(Math.abs(t.getTime() - cluster.trades.getFirst().getTime()) > CLUSTER_WAIT_TIME || cluster.isSpreadLeg != t.isSpreadLeg()){
	    		    		// most recent cluster is outside the cluster interval, or we found a mismatch in spreads, so cancel timer and being processing right away
	    		    		cluster.task.cancel();
	    		    		cluster.task.run();
	    		    		createCluster(t, symbol, ticker);
	    		    	}
	    		    	else{
	    		    		contractsMap.get(symbol).addTrade(t);
	    		    	}
	    		    }
    		    }
    	    }
        }
	    catch(StreamCorruptedException e){
	    	System.out.println("Stream corrupted");
	    	e.printStackTrace();
	    }
	    catch(IOException e){
	    	e.printStackTrace();
	    }
	    catch(ClassNotFoundException e){
	    	e.printStackTrace();
	    }
	    catch(Exception e){
	    	e.printStackTrace();
	    }
	    finally{
		    tradeOut.close();
		    jedis.close();
	    }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
    
    private void createCluster(TimeAndSale t, String symbol, String ticker){
    	Cluster cluster = new Cluster(t);
    	ClusteringTask clusterTask = new ClusteringTask(cluster);
    	cluster.task = clusterTask;
    	contractsMap.put(symbol, cluster);
    	if(t.isSpreadLeg()){
	    	// spread tracking logic, add spread leg for this ticker
    		synchronized(spreadTracker){
    			if(t.getSize() > CLUSTER_QUANTITY_THRESHOLD){
					System.out.println(String.format("NEW SPREAD TRADE FOUND\t%s\t%d\t%f\t%f\t%f\t%d", t.getEventSymbol(), t.getTime(), t.getBidPrice(), t.getAskPrice(), t.getPrice(), t.getSize()));
				}
//    			if(!spreadsMap.containsKey(ticker)){
//
//    				spread = new SpreadTracker();
//		    		spreadsMap.put(ticker, spread);
//	    		}
//	    		else{
//    				if(t.getSize() > CLUSTER_QUANTITY_THRESHOLD){
//    					System.out.println(String.format("NEW SPREAD TRADE FOUND\t%s\t%d\t%f\t%f\t%f\t%d", t.getEventSymbol(), t.getTime(), t.getBidPrice(), t.getAskPrice(), t.getPrice(), t.getSize()));
//    				}
//	    			spread = spreadsMap.get(ticker);
//	    			System.out.println(String.format("OLD SPREAD TRADE FOUND (%d)\t%s\t%d\t%f\t%f\t%f\t%d", spread.legs.size(), t.getEventSymbol(), t.getTime(), t.getBidPrice(), t.getAskPrice(), t.getPrice(), t.getSize()));	    			
//	    			spread.addCluster(cluster);
//	    		}
    			Bin bin = spreadTracker.addCluster(cluster);
    			clusterTask.addBin(bin);
    		}
    	}
    	timer.schedule(clusterTask, CLUSTER_WAIT_TIME);
    }
}