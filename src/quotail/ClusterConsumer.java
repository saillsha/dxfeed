package quotail;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.dxfeed.api.DXFeed;
import com.dxfeed.event.market.Summary;
import com.dxfeed.event.market.Trade;
import com.dxfeed.promise.Promise;

public class ClusterConsumer implements Runnable{
	private static PrintWriter clusterOut = null;
    private static Producer<String, String> producer = null;
    public static DXFeed feed = DXFeed.getInstance();

    private SpreadTracker spreadTracker;
	private Map<String, Cluster> clusterMap;
	private Map<String, Integer> uniqueMap;
	private LinkedBlockingDeque<Cluster> clusterQueue;
	private final int CLUSTER_WAIT_TIME = 400;
	private final int CLUSTER_TIMEOUT = 2000;
    private final int CLUSTER_QUANTITY_THRESHOLD = 100;
    private final int CLUSTER_MONEY_THRESHOLD = 50000;
	private final String TOPIC = "clusters";
	private SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

	private Jedis cache_client;
	private Jedis pubsub_client = new Jedis("localhost", 6380);
    public ClusterConsumer(LinkedBlockingDeque<Cluster> clusterQueue, Map<String, Cluster> clusterMap, SpreadTracker spreadTracker, String clusterFile){
		this.clusterQueue = clusterQueue;
		this.clusterMap = clusterMap;
		this.spreadTracker = spreadTracker;
		cache_client = ClusterProducer.jedisPool.getResource();

		if(producer == null)
    		configureKafkaProducer();
		try{
        	if(clusterFile != null){
        		clusterOut = new PrintWriter(new BufferedWriter(new FileWriter(clusterFile)));
        	}
        }
        catch(IOException e){
        	e.printStackTrace();
        }
	}
	
	public void run(){
		Cluster nextCluster;
		try{
			while((nextCluster = clusterQueue.take()) != null){
				long lastClusterTime = clusterQueue.isEmpty() ? nextCluster.trades.getFirst().getTime() : clusterQueue.getLast().trades.getLast().getTime();
				long nextClusterTime = nextCluster.trades.getFirst().getTime();
				
				// reinsert the next cluster to the end of the queue if it hasn't "matured", or begin processing
				if(lastClusterTime - nextCluster.trades.getFirst().getTime() > CLUSTER_WAIT_TIME ||
						System.currentTimeMillis() - nextCluster.creationTime > CLUSTER_TIMEOUT){
					// discard the cluster if it has already been processed
					boolean wasProcessed = false;
					synchronized(nextCluster){
						if(!nextCluster.isProcessed){
							nextCluster.isProcessed = true;
						}
						else
							wasProcessed = true;
					}
					if(!wasProcessed){
						processCluster(nextCluster);
					}
				}
				else {
					clusterQueue.put(nextCluster);
				}
			}
		}catch(InterruptedException e){
			e.printStackTrace();
		}
        finally{
	    	if(clusterOut != null)
	    		clusterOut.close();
        }
	}

    public void configureKafkaProducer(){
    	Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		// we have the option of overriding the default serializer here, could this be used to remove the casting that the consumer has to do?
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
    }
	
	private long elapsedTime(Cluster cluster){
		return System.currentTimeMillis() - cluster.creationTime;
	}
	
	public void processCluster(Cluster cluster){
		cluster.isProcessed = true;
		String symbol = cluster.trades.getFirst().getEventSymbol();
		String contractSymbol = symbol;
		String ticker = DXFeedUtils.getTicker(symbol);
		if(cluster.isSpreadLeg){
			symbol += ":spread";
		}
		try{
    		synchronized(clusterMap){
				clusterMap.remove(symbol);
    		}

    		if(cluster.quantity >= CLUSTER_QUANTITY_THRESHOLD){
				cluster.classifyCluster();
				String denormalizedSymbol = DXFeedUtils.denormalizeContract(symbol);
				Date d = new Date(cluster.trades.get(0).getTime());
				String oi = cache_client.hmget(df.format(d) + "_" + ticker + "_oi", contractSymbol).get(0);
				System.out.println(contractSymbol + "\t" + oi);
				cluster.openinterest = oi.equals("null") ? -1 : Integer.parseInt(oi);
	    		cluster.volume += cluster.quantity;
    			System.out.println("CLUSTER FOUND (" + elapsedTime(cluster) + "ms) [" + cluster.toJSON() + "]");
	    		if(cluster.isSpreadLeg){
	    			processSpreadLeg(cluster, ticker);
	    		}
	    		else{
//	    			KeyedMessage<String, String> message = new KeyedMessage<String, String>("clusters", DXFeedUtils.getTicker(symbol), "[" + cluster.toJSON() + "]");
//	    			producer.send(message);
	    			pubsub_client.publish("dev_rawClusters", "[" + cluster.toJSON() + "]");
	    			if(clusterOut != null){
		    			synchronized(clusterOut){
			    			// write out cluster to file
			    			clusterOut.println("[" + cluster.toJSON() + "]");
			    			clusterOut.flush();
		    			}
		    		}
	    		}
    		}
    		else if(cluster.isSpreadLeg){
    			processSpreadLeg(cluster, ticker);
    		}
		}catch(NullPointerException e){
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private void processSpreadLeg(Cluster cluster, String ticker){
		// if we have a spread leg, see if it's greater than the quantity threshold
		// if so, then increment the number of processed legs, otherwise remove it from the bin
		// afterwards, if the number of processed legs equals the size of the bin, delete the bin
		// and send it out if the bin size is greater than 0
		boolean isSpreadProcessed;
		String spreadStr = "";
		Bin bin = cluster.bin;
		synchronized(bin){
			if(cluster.quantity >= CLUSTER_QUANTITY_THRESHOLD){
				bin.incrProcessed();
    			System.out.println(String.format("SPREAD CLUSTER FOUND (%d/%d)\t%s\t%d\t%f\t%f\t%f\t%d", bin.numProcessed, bin.legs.size(),
    					cluster.trades.get(0).getEventSymbol(), cluster.trades.get(0).getTime(), cluster.trades.get(0).getBidPrice(),
    					cluster.trades.get(0).getAskPrice(), cluster.trades.get(0).getPrice(), cluster.quantity));
			}
			else
				bin.legs.remove(cluster);
			isSpreadProcessed = bin.isProcessed();
			if(isSpreadProcessed){
				spreadTracker.removeBin(bin, ticker);
				spreadStr = bin.toString();
			}
		}
		if(isSpreadProcessed && bin.legs.size() > 0){
			System.out.println("Spread PROCESSED: " + spreadStr);
//			KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, ticker, spreadStr);
//			producer.send(message);
			pubsub_client.publish("dev_rawClusters", spreadStr);
			if(clusterOut != null){
    			synchronized(clusterOut){
	    			// write out cluster to file
	    			clusterOut.println(spreadStr);
	    			clusterOut.flush();
    			}
    		}
		}
	}
}
