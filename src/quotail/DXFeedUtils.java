package quotail;
import java.util.Date;

import com.dxfeed.event.market.Side;
import com.dxfeed.event.market.TimeAndSale;
import com.dxfeed.event.market.TimeAndSaleType;

public class DXFeedUtils {
	final static int NUM_FIELDS = 10;
	final static char DELIM = '\t';
	static final int START_MIN = 9*60 + 30;
	static final int END_MIN = 16*60 + 15;
	
	public static String getHeader(){
		return String.format("SYMBOL%cTIME%cSEQ%cEX%cSIZE%cPRICE%cBID%cASK%cSIDE%cSPREAD",
				DELIM, DELIM, DELIM, DELIM, DELIM, DELIM, DELIM, DELIM, DELIM);
	}
	
	public static String serializeTrade(TimeAndSale t){
    	return t.getEventSymbol() + DELIM + t.getTime() + DELIM + t.getSequence() + DELIM +
    			t.getExchangeCode() + DELIM + t.getSize() + DELIM + t.getPrice() + DELIM + 
    			t.getBidPrice() + DELIM + t.getAskPrice() + DELIM +
    			(t.getAggressorSide() == Side.BUY ? "BUY" : (t.getAggressorSide() == Side.SELL ? "SELL" : "MID")) + DELIM +
    			t.isSpreadLeg() + DELIM + t.getType() + DELIM + t.getExchangeSaleConditions();
    }
    
    public static TimeAndSale parseTrade(String line){
    	if(line == null) return null;
    	System.out.println("line not null");
    	String[] fields = line.split(""+DELIM);
    	if(fields.length == 0) return null;

    	TimeAndSale t = new TimeAndSale(fields[0]);
    	t.setTime(Long.parseLong(fields[1]));
    	t.setSequence(Integer.parseInt(fields[2]));
    	t.setExchangeCode(fields[3].charAt(0));
    	t.setSize(Long.parseLong(fields[4]));
    	t.setPrice(Double.parseDouble(fields[5]));
    	t.setBidPrice(Double.parseDouble(fields[6]));
    	t.setAskPrice(Double.parseDouble(fields[7]));
    	t.setAggressorSide(fields[8].equals("BUY") ? Side.BUY : (fields[8].equals("SELL") ? Side.SELL : Side.UNDEFINED));
    	t.setSpreadLeg(fields[9].equals("true"));
    	t.setType(TimeAndSaleType.NEW);
    	if(fields.length >= 11){
    		if(fields[10].equals("CANCEL"))
    			t.setType(TimeAndSaleType.CANCEL);
    		else if(fields[10].equals("CORRECTION"))
    			t.setType(TimeAndSaleType.CORRECTION);
    	}
    	if(fields.length == 12){
    		t.setExchangeSaleConditions(fields[11]);
    	}
    	return t;
    }
    
    // expects a normalized contract symbol of the form VXX150626C00019000, returns VXX
    public static String getTicker(String contract){
    	if(contract.charAt(0) == '.'){
    		contract = normalizeContract(contract);
    	}
    	return contract.substring(0, contract.length() - 15);
    }

    // take something like .AAC150821C22.5 and convert to AAC150821C00022500
    public static String normalizeContract(String symbol){
    	if(symbol.charAt(0) != '.') return symbol;
    	int lastC = symbol.lastIndexOf('C');
    	int lastP = symbol.lastIndexOf('P');
    	int lastIndex = Math.max(lastP, lastC) + 1;
    	String normalizedSymbol = symbol.substring(1, lastIndex);
    	String[] strike = symbol.substring(lastIndex).split("\\.");
    	String decimal = "000";
    	if(strike.length > 1){
    		decimal = strike[1];
    		for(; decimal.length() < 3; decimal += "0");
    	}
    	return String.format("%s%05d%s", normalizedSymbol, Integer.parseInt(strike[0]), decimal);
    }
    
    // take something like AAC150821C00022500 to .AAC150821C22.5
    public static String denormalizeContract(String symbol){
    	symbol = "." + symbol;
    	int lastC = symbol.lastIndexOf('C');
    	int lastP = symbol.lastIndexOf('P');
    	int lastIndex = Math.max(lastP, lastC) + 1;
    	String denormalizedSymbol = symbol.substring(0, lastIndex);
    	String strike = symbol.substring(lastIndex);
    	strike = strike.substring(0, 5) + "." + strike.substring(5);
    	int i = 0, j = strike.length() - 1;
    	for(; i < strike.length() && strike.charAt(i) == '0'; ++i);
    	for(; j >= 0 && strike.charAt(j) == '0' || strike.charAt(j) == '.'; --j);
    	return denormalizedSymbol + strike.substring(i, j+1);
    }
    
    // check if the trade occurred during normal trading hours
    public static boolean isDuringMarketHours(long time){
    	Date d = new Date(time);
		int minutes = d.getHours() * 60 + d.getMinutes();
		return minutes >= START_MIN && minutes < END_MIN;
    }

    // a mini contract has a numeric character in its ticker
    // Example: .AMZN7150717C330
    public static boolean isMiniContract(String ticker){
    	char last = ticker.charAt(ticker.length() - 1);
    	return last >= '0' && last <= '9';
    }
}
