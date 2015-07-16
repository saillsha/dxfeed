package quotail;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class TradesConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
    private static String clusterFile = null;
    private static int numPartitions = 2;
    private static String zookeeperUrl = "localhost:2181";
    public static boolean drainQueue = false;
    public static boolean updateRedis = false;
    
    public TradesConsumer(String a_groupId, String a_topic, String zookeeperUrl) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zookeeperUrl, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numPartitions));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
        	HashMap<String, Cluster> clusterMap = new HashMap<String, Cluster>();
        	LinkedBlockingQueue<Cluster> clusterQueue = new LinkedBlockingQueue<Cluster>();
        	SpreadTracker spreadTracker = new SpreadTracker();
        	ClusterConsumer clusterConsumer = new ClusterConsumer(clusterQueue, clusterMap, spreadTracker, clusterFile);
        	new Thread(new ClusterProducer(stream, threadNumber, clusterQueue, clusterMap, spreadTracker, clusterConsumer)).start();
            new Thread(clusterConsumer).start();
            threadNumber++;
        }
    }
 
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
 
	static void processOptions(String[] args){
		Options options = new Options();
		Option clusterfile = OptionBuilder.withArgName("clusterfile").hasArg()
				.withDescription("command separated list of contracts to subscribe to when in time series mode")
				.create("clusterfile");
		Option partitions = OptionBuilder.withArgName("partitions").hasArg()
				.withDescription("number of partitions in the kafka topic").create("partitions");
		Option zookeeper = OptionBuilder.withArgName("zookeeper").hasArg()
				.withDescription("host of zookeeper server").create("zookeeper");
		options.addOption("drainqueue", false, "passively read in the kafka queue to quickly drain it");
		options.addOption("update_redis", false, "update redis with aggregate volume numbers. Only one process should be doing this at a time");
		options.addOption(clusterfile);
		options.addOption(partitions);
		options.addOption(zookeeper);

		CommandLineParser parser = new BasicParser();
		try{
			CommandLine cmd = parser.parse(options, args);
			drainQueue = cmd.hasOption("drainqueue");
			updateRedis = cmd.hasOption("update_redis");
			if(cmd.hasOption("clusterfile"))
				clusterFile = cmd.getOptionValue("clusterFile");
			if(cmd.hasOption("partitions"))
				numPartitions = Integer.parseInt(cmd.getOptionValue("partitions"));
			if(cmd.hasOption("zookeeper"))
				zookeeperUrl = cmd.getOptionValue("zookeeper");
		}catch(ParseException e){
			System.out.println("error parsing arguments");
			e.printStackTrace();
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("<-clusterfile FILENAME> <-partitions NUMBER> <-zookeeper URL> <--drainqueue>", options);
			System.exit(1);
		}
	}
    
    public static void main(String[] args) {
    	processOptions(args);
        String groupId = "timeandsalesConsumer";
        String topic = "timeandsales";

        TradesConsumer tradesConsumer = new TradesConsumer(groupId, topic, zookeeperUrl);
        tradesConsumer.run();
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException ie) {
// 
//        }
        while(true){}
//        example.shutdown();
    }
}
