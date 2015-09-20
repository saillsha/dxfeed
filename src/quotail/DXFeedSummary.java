package quotail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import kafka.producer.KeyedMessage;

import org.apache.commons.cli.*;

import com.dxfeed.event.market.TimeAndSale;
import com.dxfeed.promise.Promise;
import com.dxfeed.api.DXFeed;
import com.dxfeed.api.DXFeedEventListener;
import com.dxfeed.api.DXFeedTimeSeriesSubscription;
import com.dxfeed.event.market.TimeAndSaleType;


public class DXFeedSummary {
	static final int SYMBOL_COLUMN = 1;
	static final int MULTIPLIER_COLUMN = 7;
	static final int TICKER_COLUMN = 8;
	static PrintWriter fileOut;
	static long fromTime;
	
	public static void main(String[] args){
		Options options = new Options();
		Option filename = OptionBuilder.withArgName("file").hasArg()
				.withDescription("the instrument profile file path from which to read trades")
				.create("file");
		Option date = OptionBuilder.withArgName("date").hasArg()
				.withDescription("[YYYYMMDD] date for which we want data")
				.create("date");
		Option outfile = OptionBuilder.withArgName("outfile").hasArg()
				.withDescription("file path to which to write trades")
				.create("outfile");
		options.addOption(filename);
		options.addOption(date);
		options.addOption(outfile);
		CommandLineParser parser = new BasicParser();
		String instrumentFile = "";
		try{
			CommandLine cmd = parser.parse(options, args);
			if(cmd.hasOption("file") && cmd.hasOption("date")){
				String outFileName = "trades_" + cmd.getOptionValue("date");
				if(cmd.hasOption("outfile"))
					outFileName = cmd.getOptionValue("outfile");
//        		fileOut = new PrintWriter(new BufferedWriter(new FileWriter(outFileName)));
				instrumentFile = cmd.getOptionValue("file");
				DateFormat df = new SimpleDateFormat("yyyyMMdd");
				fromTime = df.parse(cmd.getOptionValue("date")).getTime();
				BufferedReader reader = new BufferedReader(new FileReader(instrumentFile));
				// it's going to be a very big list, might as well allocate up front
				List<String> symbols = new ArrayList<String>();

				// discard first line with column headers
				String line = reader.readLine();
				while( (line = reader.readLine()) != null){
					String[] columns = line.split(",");
					if(columns[MULTIPLIER_COLUMN].equals("100")){ // we're only interested in non-mini options
						symbols.add(columns[SYMBOL_COLUMN]);
					}
				}
				new DXFeedSummary(symbols);
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
		}
		catch(FileNotFoundException e){
			System.out.println("Cannot find file " + instrumentFile);
		}catch(IOException e){ e.printStackTrace(); }
		catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public DXFeedSummary(List<String> symbols){
		DXFeed feed = DXFeed.getInstance();
		DXFeedTimeSeriesSubscription<TimeAndSale> timeSeriesSub = feed.createTimeSeriesSubscription(TimeAndSale.class);
		timeSeriesSub.addEventListener(new TradeListener());
		timeSeriesSub.setFromTime(fromTime);
		String[] contracts = {".TSLA150918C260"};
		System.out.println("NUMBER OF SYMBOLS " + symbols.size());
		timeSeriesSub.addSymbols(contracts);
	}
	
	public class TradeListener implements DXFeedEventListener<TimeAndSale>{
		long toTime = fromTime + 24*60*60*1000;
		int counter = 0;
		public void eventsReceived(List<TimeAndSale> events) {
			for (TimeAndSale event : events){
				System.out.println(event);
				counter++;
				if(event.getTime() < toTime){
					event.setEventSymbol(DXFeedUtils.normalizeContract(event.getEventSymbol()));
//					fileOut.println(DXFeedUtils.serializeTrade(event));
					System.out.println(event);
				}
//				if(counter++ % 1000 == 0){
//					fileOut.flush();
//				}
			}
			System.out.println(counter);
		}
	}
}