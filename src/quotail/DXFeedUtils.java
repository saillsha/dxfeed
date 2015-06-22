package quotail;

import com.dxfeed.event.market.Side;
import com.dxfeed.event.market.TimeAndSale;

public class DXFeedUtils {
	final static int NUM_FIELDS = 10;
	final static char DELIM = '\t';
	
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
    	t.setSpreadLeg(fields[9] == "true");
    	return t;
    }
}
