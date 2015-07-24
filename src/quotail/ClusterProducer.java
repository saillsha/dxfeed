package quotail;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.Map;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;

import com.dxfeed.event.market.Side;
import com.dxfeed.event.market.TimeAndSale;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ClusterProducer implements Runnable {
	private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");

    private static Calendar calendar = new GregorianCalendar();
    private static final long REDIS_KEY_EXPIRY_TIME;
    private final int CLUSTER_WAIT_TIME = 400;
    private final int CLUSTER_QUANTITY_THRESHOLD = 50;
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
    private HashMap<String, Cluster> clusterMap;
    private LinkedBlockingDeque<Cluster> clusterQueue;
    private Map<String, String> aggVolMap = new HashMap<String, String>();
    private Map<String, Long> contractVolMap = new HashMap<String, Long>();
    private SpreadTracker spreadTracker = new SpreadTracker();
    private ClusterConsumer clusterConsumer;
    
    public ClusterProducer(KafkaStream a_stream, int a_threadNumber, LinkedBlockingDeque<Cluster> clusterQueue,
    		HashMap<String, Cluster> clusterMap, SpreadTracker spreadTracker, ClusterConsumer clusterConsumer) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        this.clusterMap = clusterMap;
        this.clusterQueue = clusterQueue;
        this.spreadTracker = spreadTracker;
        this.clusterConsumer = clusterConsumer;
    	aggVolMap.put("CA", "0");
    	aggVolMap.put("CB", "0");
    	aggVolMap.put("CM", "0");
    	aggVolMap.put("PA", "0");
    	aggVolMap.put("PB", "0");
    	aggVolMap.put("PM", "0");
    }
    public void run() {
    	System.out.println("starting thread..." + m_threadNumber);
    	Jedis jedis = TradesConsumer.updateRedis ? jedisPool.getResource() : null;
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
    		    // invalid trade if size 0, continue
    		    if(t.getSize() == 0 || TradesConsumer.drainQueue){
    		    	System.out.println(t);
    		    	continue;
    		    }
    		    symbol = t.getEventSymbol();
       		    ticker = DXFeedUtils.getTicker(symbol);
       		    long contractVol = 0;

       		    if(contractVolMap.containsKey(symbol)){
       		    	contractVol = contractVolMap.get(symbol);
       		    }
       		    contractVolMap.put(symbol, contractVol + t.getSize());
    		    // update redis with aggregate counts
       		    if(TradesConsumer.updateRedis){
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
       		    }

       		    if(t.isSpreadLeg()){
       		    	symbol += ":spread";
       		    }
		    	synchronized(clusterMap){
    		    	if(!clusterMap.containsKey(symbol)){
    		    		// create new cluster for this trade if none exists yet for the contract
    		    		createCluster(t, symbol, ticker, contractVol);
	    		    }
	    		    else{
	    		    	Cluster cluster = clusterMap.get(symbol);
	    		    	if(Math.abs(t.getTime() - cluster.trades.getFirst().getTime()) > CLUSTER_WAIT_TIME){
	    		    		// most recent cluster is outside the cluster interval, or we found a mismatch in spreads, being processing right away
	    		    		boolean wasProcessed = false;
	    		    		synchronized(cluster){
	    		    			// synchronize access to processing cluster so the cluster consumer thread does not attempt to do so at the same time
		    		    		if(!cluster.isProcessed){
		    		    			cluster.isProcessed = true;
		    		    		}
		    		    		else
		    		    			wasProcessed = true;
		    		    	}
	    		    		if(!wasProcessed){
	    		    			clusterConsumer.processCluster(cluster);
	    		    			createCluster(t, symbol, ticker, contractVol);
	    		    		}
	    		    	}
	    		    	else{
	    		    		clusterMap.get(symbol).addTrade(t);
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
		    jedis.close();
	    }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
    
    private void createCluster(TimeAndSale t, String symbol, String ticker, long contractVol) throws InterruptedException{
    	Cluster cluster = new Cluster(t);
    	cluster.volume = contractVol;
    	if(t.isSpreadLeg()){
    		// spread tracking logic, add spread leg for this ticker
    		synchronized(spreadTracker){
    			if(t.getSize() >= CLUSTER_QUANTITY_THRESHOLD){
					System.out.println(String.format("NEW SPREAD TRADE FOUND\t%s\t%d\t%f\t%f\t%f\t%d", t.getEventSymbol(), t.getTime(), t.getBidPrice(), t.getAskPrice(), t.getPrice(), t.getSize()));
				}
    			Bin bin = spreadTracker.addCluster(cluster);
    			cluster.bin = bin;
    		}
    	}
//    	System.out.println("adding new cluster for trade: " + t);
    	clusterMap.put(symbol, cluster);
    	clusterQueue.put(cluster);
    }
}