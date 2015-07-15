package quotail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

// class manages the number of legs in a spread and the number that have been processed
// a spread will only be pushed out to kafka when all of its legs have been processed
public class SpreadTracker {
	HashMap<String, LinkedBlockingQueue<Bin>> spreads = new HashMap<String, LinkedBlockingQueue<Bin>>();
	final int SPREAD_TIME_THRESHOLD = 5;
	public Bin findBin(Cluster cluster, String ticker){
		LinkedBlockingQueue<Bin> bins = spreads.get(ticker);
		for(Bin bin: bins){
			if(Math.abs( bin.time - cluster.trades.getFirst().getTime() ) < SPREAD_TIME_THRESHOLD){
				return bin;
			}
		}
		return null;
	}
	
	public Bin addCluster(Cluster cluster){
		String ticker = DXFeedUtils.getTicker(cluster.trades.get(0).getEventSymbol());
		Bin retval;
		if(!spreads.containsKey(ticker)){
			retval = new Bin(cluster);
			LinkedBlockingQueue<Bin> bins = new LinkedBlockingQueue<Bin>();
			bins.offer(retval);
			spreads.put(ticker, bins);
		}
		else{
			LinkedBlockingQueue<Bin> bins = spreads.get(ticker);
			retval = findBin(cluster, ticker);
			if(retval == null){
				retval = new Bin(cluster);
				bins.add(retval);
			}
			else{
				retval.legs.add(cluster);
			}
		}
		return retval;
	}
	
	public void removeBin(Bin target, String ticker){
		LinkedBlockingQueue<Bin> bins = spreads.get(ticker);
		bins.remove(target);
	}
}