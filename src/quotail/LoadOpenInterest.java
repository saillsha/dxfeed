package quotail;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.commons.cli.*;

import com.devexperts.util.DayUtil;
import com.dxfeed.event.market.Summary;
import com.dxfeed.promise.Promise;
import com.dxfeed.promise.Promises;
import com.dxfeed.api.DXFeed;

public class LoadOpenInterest {
	static final int BATCH_SIZE = 1000;
	static final int SYMBOL_COLUMN = 1;
	static final int MULTIPLIER_COLUMN = 7;
	static final int TICKER_COLUMN = 8;
	static final int EXPIRATION = 24*60*60;
	static HashMap<String, ArrayList<Summary>> contractsMap = new HashMap<String, ArrayList<Summary>>();
	private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
	private static DXFeed feed = DXFeed.getInstance();
	
	public static void processPromises(List<String> symbols){
		List<Promise<?>> promises = new ArrayList<Promise<?>>(symbols.size());
		for(String s : symbols){
			promises.add(feed.getLastEventPromise(Summary.class, s));
		}
		if (!Promises.allOf(promises).awaitWithoutException(3, TimeUnit.SECONDS)){
			System.out.println("operation timed out..calling them individually");
			for(String s : symbols){
				Promise<?> promise = feed.getLastEventPromise(Summary.class, s);
				if(promise.awaitWithoutException(200, TimeUnit.MILLISECONDS)){
					addSummary((Summary)promise.getResult());
				}
				else{
					System.out.println("failed to fetch summary for " + s);
				}
			}
		}
		else{
			for(int i = 0; i < promises.size(); ++i){
				// put the results in their respective ticker bins
				Summary s = (Summary)promises.get(i).getResult();
				addSummary(s);
			}
		}
		symbols.clear();
	}
	
	// write out the sumamry event to the redis hash
	public static void addSummary(Summary s){
		String ticker = DXFeedUtils.getTicker(s.getEventSymbol());
		try{
			contractsMap.get(ticker).add(s);
		}
		catch(NullPointerException e){
			System.out.println("error loading ticker");
		}
	}
	
	public static void main(String[] args){
		Jedis jedis = jedisPool.getResource();
		Options options = new Options();
		Option filename = OptionBuilder.withArgName("file").hasArg()
				.withDescription("the instrument profile file path from which to read trades")
				.create("file");
		options.addOption(filename);
		CommandLineParser parser = new BasicParser();
		String instrumentFile = "";
		try{
			CommandLine cmd = parser.parse(options, args);
			if(cmd.hasOption("file")){
				instrumentFile = cmd.getOptionValue("file");
				BufferedReader reader = new BufferedReader(new FileReader(instrumentFile));
				// it's going to be a very big list, might as well allocate up front
				List<String> symbols = new ArrayList<String>(BATCH_SIZE);

				// discard first line with column headers
				String line = reader.readLine();
				int  i = 0;
				while( (line = reader.readLine()) != null){
					++i;
					String[] columns = line.split(",");
					if(!columns[MULTIPLIER_COLUMN].equals("100")) continue; // we're only interested in non-mini options
					if(i % BATCH_SIZE == 0){
						System.out.println(i + " " + columns[SYMBOL_COLUMN]);
						processPromises(symbols);
					}
					String root_symbol = DXFeedUtils.getTicker(columns[SYMBOL_COLUMN]);
					if(!contractsMap.containsKey(root_symbol)) {
						contractsMap.put(root_symbol, new ArrayList<Summary>());
					}
					symbols.add(columns[SYMBOL_COLUMN]);
				}
				// process last set of promises
				processPromises(symbols);
				// create hash map to upload to redis
				for(String ticker : contractsMap.keySet()){
					Map<String, String> oiMap = new HashMap<String, String>();
					int date = 0;
					int counter = 0;
					for(Summary summary : contractsMap.get(ticker)){
						date = DayUtil.getYearMonthDayByDayId(summary.getDayId());
						String day = "" + date;
						day = day.substring(0, 4) + "-" + day.substring(4, 6) + "-" + day.substring(6);
						String symbol = DXFeedUtils.normalizeContract(summary.getEventSymbol());
						oiMap.put(symbol, ""+summary.getOpenInterest());
					}
					counter += contractsMap.get(ticker).size();
					System.out.println(counter + " " + ticker);
					try{
						String key = "" + date + "_" + ticker + "_oi";
						jedis.hmset(key, oiMap);
						jedis.expire(key, EXPIRATION);
					}
					catch(Exception e){
						System.out.println("error getting summary event");
					}
				}
				System.out.println("program completed successfully");
				System.exit(0);
			}
			else{
				System.out.println("instrument profile path must be specified with -f option");
				System.exit(1);
			}
		}catch(ParseException e){
			System.out.println("error parsing arguments");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("<-f FILENAME>", options);
			System.exit(1);
		}catch(FileNotFoundException e){
			System.out.println("Cannot find file " + instrumentFile);
		}catch(IOException e){ e.printStackTrace(); }
	}
}
