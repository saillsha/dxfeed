package quotail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
import com.dxfeed.api.DXFeedSubscription;

public class DXFeedSummary {
	static final int INITIAL_CAPACITY = 800500;
	static final int SYMBOL_COLUMN = 1;
	static final int MULTIPLIER_COLUMN = 7;
	static final int TICKER_COLUMN = 8;
	static HashMap<String, ArrayList<Summary>> contractsMap = new HashMap<String, ArrayList<Summary>>();
	private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");

	
	public static void processPromises(List<Promise<?>> promises){
		if (!Promises.allOf(promises).awaitWithoutException(10, TimeUnit.SECONDS)){
			System.out.println("operation timed out");
		}
		else{
			for(int i = 0; i < promises.size(); ++i){
				// put the results in their respective ticker bins
				Summary s = (Summary)promises.get(i).getResult();
				String ticker = DXFeedUtils.getTicker(s.getEventSymbol());
				try{
					contractsMap.get(ticker).add(s);
				}
				catch(NullPointerException e){
					System.out.println("error");
				}
			}
			// write this out to redis as hash
		}
		promises.clear();
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
				DXFeed feed = DXFeed.getInstance();
				BufferedReader reader = new BufferedReader(new FileReader(instrumentFile));
				// it's going to be a very big list, might as well allocate up front
				List<Promise<?>> promises = new ArrayList<Promise<?>>(INITIAL_CAPACITY);

				// discard first line with column headers
				String line = reader.readLine();
				int  i = 0;

				while( (line = reader.readLine()) != null && i < 5000){
					if(i % 100 == 0){
						processPromises(promises);
					}
					String[] columns = line.split(",");
					if(!columns[MULTIPLIER_COLUMN].equals("100")) continue; // we're only interested in non-mini options
					System.out.println(i + " " + columns[SYMBOL_COLUMN]);
					String root_symbol = DXFeedUtils.getTicker(columns[SYMBOL_COLUMN]);
					if(!contractsMap.containsKey(root_symbol)) {
						contractsMap.put(root_symbol, new ArrayList<Summary>());
					}
					promises.add(feed.getLastEventPromise(Summary.class, columns[SYMBOL_COLUMN]));
					++i;
				}
				// process last set of promises
				processPromises(promises);
				for(String ticker : contractsMap.keySet()){
					Map<String, String> oiMap = new HashMap<String, String>();
					int date = 0;
					if(ticker.equals("AAPL7")){
						ArrayList<Summary> s = contractsMap.get(ticker);
						System.out.println("whoa nelly");
					}
					for(Summary summary : contractsMap.get(ticker)){
						date = DayUtil.getYearMonthDayByDayId(summary.getDayId());
						String day = "" + date;
						day = day.substring(0, 4) + "-" + day.substring(4, 6) + "-" + day.substring(6);
						String symbol = DXFeedUtils.normalizeContract(summary.getEventSymbol());
						String json = String.format("{\"date\": \"%s\", \"symbol\": \"%s\", \"openinterest\": \"%d\"}",
								day, symbol, summary.getOpenInterest());
						oiMap.put(symbol, json);
					}

					System.out.println(ticker);
					jedis.hmset("" + date + "_" + ticker + "_chains", oiMap);
				}
				
//        		PrintWriter outfile = new PrintWriter(new BufferedWriter(new FileWriter("/Users/sahil/Documents/workspace/dxfeed/sample_files/tickers.txt", true)));
//				outfile.close();

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
