package quotail;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.BufferedWriter;

import com.dxfeed.event.market.Side;
import com.dxfeed.event.market.TimeAndSale;

import org.codehaus.jackson.map.ObjectMapper;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class ConsumerWorkerThread implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private PrintWriter out;
    private PrintWriter clusterOut;
    private boolean printTrades;
    private final int CLUSTER_WAIT_TIME = 2000;
    private final int CLUSTER_QUANTITY_THRESHOLD = 50;
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, Cluster> contractsMap;
    Map<String, String> aggVolMap = new HashMap<String, String>();
    Timer timer;
    JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
    
    public ConsumerWorkerThread(KafkaStream a_stream, int a_threadNumber, boolean printTrades) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        timer = new Timer(true);
        this.printTrades = printTrades;
        contractsMap = new HashMap<String, Cluster>();
    	aggVolMap.put("CA", "0");
    	aggVolMap.put("CB", "0");
    	aggVolMap.put("CM", "0");
    	aggVolMap.put("PA", "0");
    	aggVolMap.put("PB", "0");
    	aggVolMap.put("PM", "0");
        try{
        	out = new PrintWriter(new BufferedWriter(new FileWriter("/Users/sahil/Documents/workspace/dxfeed/trades.txt", true)));
        	clusterOut = new PrintWriter(new BufferedWriter(new FileWriter("/Users/sahil/Documents/workspace/dxfeed/clusters.txt", true)));
        }
        catch(IOException e){
        	e.printStackTrace();
        }
    }

    private class ClusteringTask extends TimerTask{
    	private String symbol;
    	private TimeAndSale newTrade = null;
    	public ClusteringTask(String symbol){
    		this.symbol = symbol;
    	}
    	// overloaded constructor for when we find a new trade that is 
    	// more than the cluster interval away from the first one, 
    	// because then we want to start a new cluster with the latest trade
    	public ClusteringTask(String symbol, TimeAndSale newTrade){
    		this.symbol = symbol;
    		this.newTrade = newTrade;
    	}
    	public void run(){
    		Cluster cluster;
    		try{
	    		synchronized(contractsMap){
					cluster = contractsMap.get(symbol);
					if(cluster == null || cluster.trades == null || cluster.trades.getFirst() == null){
						System.out.println("we got a problem over here");
					}
					contractsMap.remove(symbol);
					if(newTrade != null){
						Cluster newCluster = new Cluster(newTrade);
						newCluster.task = new ClusteringTask(symbol);
						timer.schedule(newCluster.task, CLUSTER_QUANTITY_THRESHOLD);
					}
	    		}
				if(cluster.quantity >= CLUSTER_QUANTITY_THRESHOLD){
		    		System.out.println("cluster found " + DXFeedUtils.serializeTrade(cluster.trades.getFirst()));
					cluster.classifyCluster();
		    		clusterOut.println(cluster.toJSON());
		    		clusterOut.flush();
	    		}
			}catch(NullPointerException e){ 
				e.printStackTrace();
			}
    	}
    }
    
    public void run() {
    	System.out.println("starting thread..." + m_threadNumber);
    	ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()){
        	byte[] serializedTrade = it.next().message();
         	ByteArrayInputStream in = new ByteArrayInputStream(serializedTrade);
    	    TimeAndSale t = null;
    	    Jedis jedis = null;
    	    try{
    		    ObjectInputStream is = new ObjectInputStream(in);
    		    t = (TimeAndSale)is.readObject();
    		    String symbol = t.getEventSymbol();
    		    if(t.getSize() == 0) continue;
    		    if(printTrades){
    		    	out.println(DXFeedUtils.serializeTrade(t));
    		    }
    		    jedis = jedisPool.getResource();
    		    String ticker = DXFeedUtils.getTicker(t.getEventSymbol());
    		    char optionType = t.getEventSymbol().lastIndexOf('C', 6) == -1 ? 'P' : 'C';
		    	String key = optionType + (t.getAggressorSide() == Side.BUY ? "A" : (t.getAggressorSide() == Side.SELL ? "B" : "M"));

		    	// update redis with aggregate counts
		    	Map<String, String> updatedVol = new HashMap<String, String>();
		    	if(jedis.exists(ticker)){
    		    	int aggVol = Integer.parseInt(jedis.hmget(ticker, key).get(0));
    		    	aggVol += t.getSize();
    		    	updatedVol.put(key, "" + aggVol);
    		    }
    		    else{
    		    	updatedVol.putAll(aggVolMap);
    		    	updatedVol.put(key, "" + (Integer.parseInt(aggVolMap.get(key)) + t.getSize()));
    		    }
		    	jedis.hmset(ticker, updatedVol);

		    	synchronized(contractsMap){
	    		    // synchronized access to shared contractsMap
        	      	System.out.println(t);
    		    	if(!contractsMap.containsKey(symbol)){
	    		    	Cluster cluster = new Cluster(t);
	    		    	cluster.task = new ClusteringTask(symbol);
	    		    	contractsMap.put(symbol, cluster);
	    		    	timer.schedule(cluster.task, CLUSTER_WAIT_TIME);
	    		    }
	    		    else{
	    		    	Cluster cluster = contractsMap.get(symbol);
	    		    	if(Math.abs(t.getTime() - cluster.trades.getFirst().getTime()) > CLUSTER_WAIT_TIME){
	    		    		// most recent cluster is outside the cluster interval, so cancel timer and being processing right away
	    		    		cluster.task.cancel();
	    		    		timer.schedule(new ClusteringTask(symbol, t), 1);
	    		    	}
	    		    	else{
	    		    		contractsMap.get(symbol).addTrade(t);
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
    	    	jedis.close();
    	    }
        }
	    out.close();
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}