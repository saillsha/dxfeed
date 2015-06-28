package quotail;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.net.Socket;
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
    private final int CLUSTER_WAIT_TIME = 2000;
    private final int CLUSTER_QUANTITY_THRESHOLD = 50;
    private final long SUMMARY_TIMEOUT = 200;
	private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
    private static PrintWriter tradeOut = null;
    private static PrintWriter clusterOut = null;
    private static Producer<String, String> producer = null;
    public static DXFeed feed = DXFeed.getInstance();
    private static Timer timer;

	private KafkaStream m_stream;
    private int m_threadNumber;

    private HashMap<String, Cluster> contractsMap;
    private Map<String, String> aggVolMap = new HashMap<String, String>();
    private HashMap<String, SpreadTracker> spreadsMap = new HashMap<String, SpreadTracker>();
    
    public void configureKafkaProducer(){
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		// we have the option of overriding the default serializer here, could this be used to remove the casting that the consumer has to do?
		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("partitioner.class", "quotail.TickerPartitioner");
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

    // class manages the number of legs in a spread and the number that have been processed
    // a spread will only be pushed out on the socket when all of its legs have been processed
    private class SpreadTracker {
    	public List<Cluster> legs = new ArrayList<Cluster>();
    	public int numProcessed = 0;
    	public SpreadTracker(Cluster cluster){
    		legs.add(cluster);
    	}
    	public void reset(){
    		legs.clear();
    		numProcessed = 0;
    	}
    	public void addLeg(Cluster cluster){ legs.add(cluster); }
    	public void incrProcessed(){ ++this.numProcessed; }
    	public String toString(){
    		StringBuilder sb = new StringBuilder();
    		sb.append("[");
    		for(Cluster cluster : legs){
    			sb.append(cluster.toJSON());
    			sb.append(",");
    		}
    		sb.deleteCharAt(sb.length() - 1);
    		sb.append("]");
    		return sb.toString();
    	}
    	public boolean isProcessed(){ return numProcessed == legs.size(); }
    }
    
    private class ClusteringTask extends TimerTask{
    	private Cluster cluster;
    	private String symbol;
    	private TimeAndSale newTrade = null;
    	private SpreadTracker spread;
    	public ClusteringTask(Cluster cluster){
    		this.cluster = cluster;
    		this.symbol = cluster.trades.getFirst().getEventSymbol();
    	}
    	
    	public void addSpread(SpreadTracker spread){ this.spread = spread; }
    	
    	public void run(){
    		try{
	    		synchronized(contractsMap){
					contractsMap.remove(symbol);
	    		}
				if(cluster.quantity >= CLUSTER_QUANTITY_THRESHOLD){
		    		cluster.classifyCluster();
		    		Promise<Summary> summaryPromise = feed.getLastEventPromise(Summary.class, symbol);
		    		if(summaryPromise.awaitWithoutException(SUMMARY_TIMEOUT, TimeUnit.MILLISECONDS)){
			    		cluster.openinterest = summaryPromise.getResult().getOpenInterest();		    			
		    		}
		    		if(cluster.isSpreadLeg){
		    			spread.incrProcessed();
		    			System.out.println("spread found " + symbol + " " + spread.numProcessed + "/" + spread.legs.size());
		    			if(spread.isProcessed()){
			    			System.out.println("Spread PROCESSED: " + spread.toString());
		    				KeyedMessage<String, String> message = new KeyedMessage<String, String>("clusters", DXFeedUtils.getTicker(symbol), spread.toString());
		    				producer.send(message);
		    			}
		    		}
		    		else{
		    			System.out.println("[" + cluster.toJSON() + "]");
	    				KeyedMessage<String, String> message = new KeyedMessage<String, String>("clusters", DXFeedUtils.getTicker(symbol), "[" + cluster.toJSON() + "]");
	    				producer.send(message);
		    		}
		    		if(clusterOut != null){
		    			// write out cluster to file
		    			clusterOut.println(cluster.toJSON());
			    		clusterOut.flush();
		    		}
	    		}
				else if(cluster.isSpreadLeg){
					// reset the spread count for non-clusters
					spreadsMap.get(DXFeedUtils.getTicker(symbol)).reset();
				}
			}catch(NullPointerException e){
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
    		    System.out.println(m_threadNumber + "\t" + t);
    		    
    		    symbol = t.getEventSymbol();
    		    ticker = DXFeedUtils.getTicker(symbol);
    		    // invalid trade if size 0, continue
    		    if(t.getSize() == 0) continue;
    		    if(tradeOut != null){
    		    	// print trade out to file if enabled in run-time options
    		    	tradeOut.println(DXFeedUtils.serializeTrade(t));
    		    }

    		    // update redis with aggregate counts
    		    char optionType = t.getEventSymbol().lastIndexOf('C', 6) == -1 ? 'P' : 'C';
		    	String key = optionType + (t.getAggressorSide() == Side.BUY ? "A" : (t.getAggressorSide() == Side.SELL ? "B" : "M"));
		    	Map<String, String> updatedVol = new HashMap<String, String>();
		    	if(jedis.exists(ticker)){
    		    	int aggVol = Integer.parseInt(jedis.hmget(ticker, key).get(0)) + (int)t.getSize();
    		    	updatedVol.put(key, "" + aggVol);
    		    }
    		    else{
    		    	updatedVol.putAll(aggVolMap);
    		    	updatedVol.put(key, "" + (Integer.parseInt(aggVolMap.get(key)) + t.getSize()));
    		    }
		    	jedis.hmset(ticker, updatedVol);

		    	synchronized(contractsMap){
    		    	if(!contractsMap.containsKey(symbol)){
    		    		// create new cluster for this trade if none exists yet for the contract
	    		    	Cluster cluster = new Cluster(t);
	    		    	ClusteringTask clusterTask = new ClusteringTask(cluster);
	    		    	cluster.task = clusterTask;
	    		    	contractsMap.put(symbol, cluster);
	    		    	timer.schedule(cluster.task, CLUSTER_WAIT_TIME);
	    		    	
	    		    	if(t.isSpreadLeg()){
		    		    	// spread tracking logic, add spread leg for this ticker
	    		    		synchronized(spreadsMap){
		    		    		if(!spreadsMap.containsKey(ticker)){
		    		    			SpreadTracker spread = new SpreadTracker(cluster);
			    		    		spreadsMap.put(ticker, spread);
			    		    		clusterTask.addSpread(spread);
		    		    		}
		    		    		else{
			    		    		spreadsMap.get(ticker).addLeg(cluster);
			    		    		clusterTask.addSpread(spreadsMap.get(ticker));
		    		    		}
		    		    	}
	    		    	}
	    		    }
	    		    else{
	    		    	Cluster cluster = contractsMap.get(symbol);
	    		    	if(Math.abs(t.getTime() - cluster.trades.getFirst().getTime()) > CLUSTER_WAIT_TIME){
	    		    		// most recent cluster is outside the cluster interval, so cancel timer and being processing right away
	    		    		cluster.task.cancel();
	    		    		cluster.task.run();
	    					Cluster newCluster = new Cluster(t);
	    					newCluster.task = new ClusteringTask(newCluster);
	    					timer.schedule(newCluster.task, CLUSTER_QUANTITY_THRESHOLD);
	    					synchronized(contractsMap){
	    						contractsMap.put(symbol, newCluster);
	    					}
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
	    finally{
		    tradeOut.close();
		    jedis.close();
	    }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}