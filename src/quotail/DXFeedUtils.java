package quotail;
import java.util.Date;

import com.dxfeed.event.market.Side;
import com.dxfeed.event.market.TimeAndSale;

public class DXFeedUtils {
	final static int NUM_FIELDS = 10;
	final static char DELIM = '\t';
	static final int START_MIN = 9*60 + 30;
	static final int END_MIN = 16*60;
	
	public static String getHeader(){
		return "SYMBOL\tTIME\tSEQ\tEX\tSIZE\tPRICE\tBID\tASK\tSIDE\tSPREAD";
	}
	
	public static String serializeTrade(TimeAndSale t){
    	return t.getEventSymbol() + DELIM + t.getTime() + DELIM + t.getSequence() + DELIM +
    			t.getExchangeCode() + DELIM + t.getSize() + DELIM + t.getPrice() + DELIM + 
    			t.getBidPrice() + DELIM + t.getAskPrice() + DELIM +
    			(t.getAggressorSide() == Side.BUY ? "BUY" : "SELL") + DELIM + t.isSpreadLeg();
    }
    
    public static TimeAndSale parseTrade(String line){
    	if(line == null) return null;
    	String[] fields = line.split(""+DELIM);
    	if(fields.length != NUM_FIELDS) return null;

    	TimeAndSale t = new TimeAndSale(fields[0]);
    	t.setTime(Long.parseLong(fields[1]));
    	t.setSequence(Integer.parseInt(fields[2]));
    	t.setExchangeCode(fields[3].charAt(0));
    	t.setSize(Long.parseLong(fields[4]));
    	t.setPrice(Double.parseDouble(fields[5]));
    	t.setBidPrice(Double.parseDouble(fields[6]));
    	t.setAskPrice(Double.parseDouble(fields[7]));
    	t.setAggressorSide(fields[8] == "BUY" ? Side.BUY : Side.SELL);
    	t.setSpreadLeg(fields[9].equals("true"));
    	return t;
    }
    
    // expects a contract symbol of the form .VXX150626C19, returns VXX
    public static String getTicker(String contract){
		// this will be true until ~2018 (when there are 2020 LEAPs)
    	return contract.substring(1, contract.indexOf('1'));
    }

    // check if the trade occurred during normal trading hours
    public static boolean isDuringMarketHours(long time){
    	Date d = new Date(time);
		int minutes = d.getHours() * 60 + d.getMinutes();
		return minutes > START_MIN && minutes < END_MIN;
    }

    // a mini contract has a numeric character in its ticker
    // Example: .AMZN7150717C330
    public static boolean isMiniContract(String ticker){
    	char last = ticker.charAt(ticker.length() - 1);
    	return last >= '0' && last <= '9';
    }
}
