package quotail;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.Date;
import java.util.Map;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.text.SimpleDateFormat;
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
	public static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
	
    private static Calendar calendar = new GregorianCalendar();
    private static final int REDIS_KEY_EXPIRY_TIME = 24*60*60;
    private final int CLUSTER_WAIT_TIME = 400;
    private final int CLUSTER_QUANTITY_THRESHOLD = 100;
//    static{
//    	// set expiry of redis keys to 9:00 of the following day
//    	calendar.add(Calendar.DATE, 1);
//		calendar.set(Calendar.HOUR, 9);
//		calendar.set(Calendar.MINUTE, 0);
//		calendar.set(Calendar.AM_PM, 0);
//    }

	private KafkaStream m_stream;
    private int m_threadNumber;
    private HashMap<String, Cluster> clusterMap;
    private LinkedBlockingDeque<Cluster> clusterQueue;
    private Map<String, String> aggVolMap = new HashMap<String, String>();
    private Map<String, Long> contractVolMap = new HashMap<String, Long>();
    private SpreadTracker spreadTracker = new SpreadTracker();
    private ClusterConsumer clusterConsumer;
    // date format object for generating redis keys
    private SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
	private Jedis jedis;
    
    public ClusterProducer(KafkaStream a_stream, int a_threadNumber, LinkedBlockingDeque<Cluster> clusterQueue,
    		HashMap<String, Cluster> clusterMap, SpreadTracker spreadTracker, ClusterConsumer clusterConsumer) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        this.clusterMap = clusterMap;
        this.clusterQueue = clusterQueue;
        this.spreadTracker = spreadTracker;
        this.clusterConsumer = clusterConsumer;
        this.jedis = TradesConsumer.updateRedis ? jedisPool.getResource() : null;
        aggVolMap.put("CA", "0");
    	aggVolMap.put("CB", "0");
    	aggVolMap.put("CM", "0");
    	aggVolMap.put("PA", "0");
    	aggVolMap.put("PB", "0");
    	aggVolMap.put("PM", "0");
    }

    // update both aggregate ticker volume and contract volume
    // ticker agg vol structure is  AAPL => {"CA": "593", "CB": "4818", "CM": "31", "PA": "1241", "PB": "83", "PM": "288"}
    // contract agg vol structure is AAPL => {"AAPL151018C00125000": "B:203,A:312,M:21", ...}
    private void updateRedisAggVol(TimeAndSale t, String ticker){
	    Date d = new Date(t.getTime());
	    char optionType = t.getEventSymbol().substring(7).lastIndexOf('C') == -1 ? 'P' : 'C';
	    char aggressorSide = t.getAggressorSide() == Side.BUY ? 'A' : (t.getAggressorSide() == Side.SELL ? 'B' : 'M');

	    String tickerHashKey = "" + optionType + aggressorSide;
	    String contractHashKey = DXFeedUtils.normalizeContract(t.getEventSymbol());
    	String tickerVolKey =  df.format(d) + "_" + ticker + "_agg_vol";
    	String contractVolKey = df.format(d) + "_" + ticker + "_contract_vol";
    	Map<String, String> updatedVol = new HashMap<String, String>();
    	Map<String, String> updatedContractVol = new HashMap<String, String>();
    	// update ticker aggregate volume
    	if(jedis.exists(tickerVolKey)){
    		String agg_vol = jedis.hmget(tickerVolKey, tickerHashKey).get(0);
    		agg_vol = agg_vol == null ? "0" : agg_vol;
	    	long contractSize = t.isCancel() ? t.getSize() * -1 : ( t.isCorrection() ? 0 : t.getSize() );
	    	long aggVol = contractSize + Integer.parseInt(agg_vol);
	    	updatedVol.put(tickerHashKey, "" + aggVol);
	    	jedis.hmset(tickerVolKey, updatedVol);
	    	jedis.expire(tickerVolKey, REDIS_KEY_EXPIRY_TIME);
    	}
	    else{
	    	updatedVol.putAll(aggVolMap);
	    	updatedVol.put(tickerHashKey, "" + t.getSize());
	    	jedis.hmset(tickerVolKey, updatedVol);
	    	jedis.expire(tickerVolKey, REDIS_KEY_EXPIRY_TIME);
	    }

    	// update contract aggregate volume
		long contractSize = t.isCancel() ? t.getSize() * -1 : (t.isCorrection() ? 0 : t.getSize());
		String contractVol = "B:0,A:0,M:0";
    	if(jedis.exists(contractVolKey)){
    		String currentVol = jedis.hmget(contractVolKey, contractHashKey).get(0);
    		if(currentVol != null){
    			contractVol = currentVol;
        		for(String s : currentVol.split(",")){
        			if(aggressorSide == s.charAt(0)){
        				contractSize += Long.parseLong(s.split(":")[1]);
        			}
        		}    			
    		}
    	}
		contractVol = contractVol.replaceFirst(aggressorSide + ":[0-9]+", aggressorSide + ":" + contractSize);
		updatedContractVol.put(contractHashKey, contractVol);
		jedis.hmset(contractVolKey, updatedContractVol);
		jedis.expire(contractVolKey, REDIS_KEY_EXPIRY_TIME);
    }
   
    
    public void run() {
    	System.out.println("starting thread..." + m_threadNumber);
    	ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
	    TimeAndSale t = null;
	    // contract and ticker symbols
	    String symbol, ticker;
	    try{
	    	while (it.hasNext()){
    	    	// convert byte[] to TimeAndSale object
	        	byte[] serializedTrade = it.next().message();
	         	ByteArrayInputStream in = new ByteArrayInputStream(serializedTrade);
    		    ObjectInputStream is = new ObjectInputStream(in);
    		    t = (TimeAndSale)is.readObject();
    		    symbol = t.getEventSymbol();
    		    ticker = DXFeedUtils.getTicker(symbol);
    		    // update redis with aggregate counts
    		    if(TradesConsumer.updateRedis){
    		    	updateRedisAggVol(t, ticker);
    		    }
    		    // continue if invalid trade of size 0 or drainqueue flag is on
    		    if(t.getSize() == 0 || TradesConsumer.drainQueue){
    		    	System.out.println(t);
    		    	continue;
    		    }
    		    
    		    long contractVol = 0;
       		    if(contractVolMap.containsKey(symbol)){
       		    	contractVol = contractVolMap.get(symbol);
       		    }
       		    contractVolMap.put(symbol, contractVol + t.getSize());

       		    if(t.isSpreadLeg()){
       		    	symbol += ":spread";
       		    }
		    	synchronized(clusterMap){
//		    		if(t.isCancel() && clusterMap.containsKey(symbol)){
//		    			Cluster cluster = clusterMap.get(symbol);
//		    			if(clusterMap.get(symbol).cancelTrade(t)){
//		    				// if we were successfully able to eliminate the error at this stage, then
//		    				// don't process the trade and move on. otherwise, it will need to be sent
//		    				// further down the pipeline
//		    				continue;
//		    			}
//		    		}
//		    		else if(t.isCorrection() && clusterMap.containsKey(symbol)){
//		    			if(clusterMap.get(symbol).correctTrade(t)){
//		    				continue;
//		    			}
//		    		}
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