package quotail;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class TickerPartitioner implements Partitioner{
	public TickerPartitioner(VerifiableProperties props){}
	
	// partition ticker based on first character
	public int partition(Object key, int numPartitions){
		String keyStr = new String((byte[])key);
		return keyStr.charAt(0) % numPartitions;
	}
}
