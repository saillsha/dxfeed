package quotail;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.net.Socket;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.BufferedWriter;

import com.dxfeed.event.market.Side;
import com.dxfeed.event.market.TimeAndSale;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class ConsumerWorkerThread implements Runnable {
    private final int CLUSTER_WAIT_TIME = 2000;
    private final int CLUSTER_QUANTITY_THRESHOLD = 50;
	private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
    private static PrintWriter tradeOut = null;
    private static PrintWriter clusterOut = null;
//    private static PrintWriter socketOut;
    private static Timer timer;

	private KafkaStream m_stream;
    private int m_threadNumber;

    private HashMap<String, Cluster> contractsMap;
    private Map<String, String> aggVolMap = new HashMap<String, String>();
    private HashMap<String, SpreadTracker> spreadsMap = new HashMap<String, SpreadTracker>();
    
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
        try{
        	if(tradeFile != null){
        		tradeOut = new PrintWriter(new BufferedWriter(new FileWriter(tradeFile, true)));
        	}
        	if(clusterFile != null){
        		clusterOut = new PrintWriter(new BufferedWriter(new FileWriter(clusterFile, true)));
        	}
//        	Socket clusterSocket = new Socket("localhost", 1337);
//        	socketOut = new PrintWriter(clusterSocket.getOutputStream(), true);
        }
        catch(IOException e){
        	e.printStackTrace();
        }
    }

    // class manages the number of legs in a spread and the number that have been processed
    // a spread will only be pushed out on the socket when all of its legs have been processed
    private class SpreadTracker {
    	private List<Cluster> legs = new ArrayList<Cluster>();
    	private int numProcessed = 0;
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
    		spread = spreadsMap.get(DXFeedUtils.getTicker(symbol));
    	}
    	// overloaded constructor for when we find a new trade that is 
    	// more than the cluster interval away from the first one, 
    	// because then we want to start a new cluster with the latest trade
    	public ClusteringTask(TimeAndSale newTrade){
    		this.newTrade = newTrade;
    	}
    	
    	public void run(){
    		Cluster cluster;
    		try{
	    		synchronized(contractsMap){
					cluster = contractsMap.get(symbol);
					contractsMap.remove(symbol);
	    		}
	    		if(cluster == null){
	    			System.out.println("NULL cluster");
	    		}
				if(cluster.quantity >= CLUSTER_QUANTITY_THRESHOLD){
		    		System.out.println("cluster found " + DXFeedUtils.serializeTrade(cluster.trades.getFirst()));
		    		cluster.classifyCluster();

		    		if(cluster.isSpreadLeg){
		    			spread.incrProcessed();
		    			if(spread.isProcessed()){
		    				// TODO: push out spread to kafka
//				    		synchronized(socketOut){
				    			System.out.println("Spread found: " + spread.toString());
//				    			socketOut.write(spread.toString());
//				    			socketOut.flush();
//				    		}
		    			}
		    		}
		    		else{
		    			// TODO: push out cluster to kafka
//			    		synchronized(socketOut){
//			    			socketOut.write("[" + cluster.toJSON() + "]");
//			    			socketOut.flush();
//			    		}
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
	    		    	cluster.task = new ClusteringTask(cluster);
	    		    	contractsMap.put(symbol, cluster);
	    		    	timer.schedule(cluster.task, CLUSTER_WAIT_TIME);
	    		    	
	    		    	if(t.isSpreadLeg()){
	    		    		System.out.println("We found a spread leg");
		    		    	// spread tracking logic, add spread leg for this ticker
	    		    		synchronized(spreadsMap){
		    		    		if(!spreadsMap.containsKey(ticker))
			    		    		spreadsMap.put(ticker, new SpreadTracker(cluster));
		    		    		else
			    		    		spreadsMap.get(ticker).addLeg(cluster);		    		    			
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