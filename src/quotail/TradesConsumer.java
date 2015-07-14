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

public class TradesConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
    private static String tradeFile = null, clusterFile = null;
    private static int numThreads = 2;
    private static String zookeeperUrl = "localhost:2181";
    
    public TradesConsumer(String a_groupId, String a_topic, String zookeeperUrl) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zookeeperUrl, a_groupId));
        this.topic = a_topic;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }
 
    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerWorkerThread(stream, threadNumber, tradeFile, clusterFile));
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
		Option tradefile = OptionBuilder.withArgName("tradefile").hasArg()
				.withDescription("the file path to which you want to write trades")
				.create("tradefile");
		Option clusterfile = OptionBuilder.withArgName("clusterfile").hasArg()
				.withDescription("command separated list of contracts to subscribe to when in time series mode")
				.create("clusterfile");
		Option threads = OptionBuilder.withArgName("threads").hasArg()
				.withDescription("number of threads to use for processing").create("threads");
		Option zookeeper = OptionBuilder.withArgName("zookeeper").hasArg()
				.withDescription("host of zookeeper server").create("zookeeper");
		options.addOption(tradefile);
		options.addOption(clusterfile);
		options.addOption(threads);
		options.addOption(zookeeper);
		CommandLineParser parser = new BasicParser();
		try{
			CommandLine cmd = parser.parse(options, args);
			if(cmd.hasOption("tradefile"))
				tradeFile = cmd.getOptionValue("tradefile");
			if(cmd.hasOption("clusterfile"))
				clusterFile = cmd.getOptionValue("clusterFile");
			if(cmd.hasOption("threads"))
				numThreads = Integer.parseInt(cmd.getOptionValue("threads"));
			if(cmd.hasOption("zookeeper"))
				zookeeperUrl = cmd.getOptionValue("zookeeper");
		}catch(ParseException e){
			System.out.println("error parsing arguments");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("<-tradefile FILENAME> <-clusterfile FILENAME> <-threads NUMBER> <-zookeeper URL>", options);
			System.exit(1);
		}
	}
    
    public static void main(String[] args) {
    	processOptions(args);
        String groupId = "timeandsalesConsumer";
        String topic = "timeandsales";

        TradesConsumer example = new TradesConsumer(groupId, topic, zookeeperUrl);
        example.run(numThreads);
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException ie) {
// 
//        }
        while(true){}
//        example.shutdown();
    }
}
