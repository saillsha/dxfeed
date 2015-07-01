package quotail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.*;

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
	
	public static void main(String[] args){
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
				HashSet<String> tickers = new HashSet<String>();
				int  i =0;
				promises.add(feed.getLastEventPromise(Summary.class, ".AA150918P5"));
//				while( (line = reader.readLine()) != null/* && i < 10000*/){
//					String[] columns = line.split(",");
//					tickers.add(columns[TICKER_COLUMN]);
//					if(columns[MULTIPLIER_COLUMN].equals("100")) // we're only interested in non-mini options
//						promises.add(feed.getLastEventPromise(Summary.class, columns[SYMBOL_COLUMN]));
//					if(columns[MULTIPLIER_COLUMN].equals("10")){
//						System.out.println(columns[SYMBOL_COLUMN]);
//						++i;
//					}
//				}
////				System.out.println("number of promises: " + promises.size());
//				System.out.println("number of symbols: " + tickers.size());
//        		PrintWriter outfile = new PrintWriter(new BufferedWriter(new FileWriter("/Users/sahil/Documents/workspace/dxfeed/sample_files/tickers.txt", true)));
//				for(String ticker: tickers){
//					outfile.println(ticker);
//				}
//				outfile.close();
				if (!Promises.allOf(promises).awaitWithoutException(1, TimeUnit.SECONDS)){
					System.out.println("operation timed out");
				}
				else{
					for(i = 0; i < promises.size(); ++i){
						System.out.println(i + " " + promises.get(i).getResult());
					}
				}
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
