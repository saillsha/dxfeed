package quotail;

import java.util.ArrayList;

public class Bin{
	long time;
	long quantity;
	int numProcessed = 0;
	ArrayList<Cluster> legs = new ArrayList<Cluster>();
	public Bin(Cluster cluster){
		legs.add(cluster);
		time = cluster.trades.getFirst().getTime();
		quantity = cluster.trades.getFirst().getSize();
	}
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
	public void incrProcessed() { ++this.numProcessed; }
	public boolean isProcessed(){ return numProcessed == legs.size(); }
}