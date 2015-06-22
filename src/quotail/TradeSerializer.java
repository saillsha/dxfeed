package quotail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import org.apache.kafka.common.serialization.Serializer;

import com.dxfeed.event.market.TimeAndSale;

public class TradeSerializer implements Encoder<TimeAndSale>, Serializer<TimeAndSale>{

    public TradeSerializer(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }
    
	public TimeAndSale fromBytes(byte[] serializedTrade) {
	    ByteArrayInputStream in = new ByteArrayInputStream(serializedTrade);
	    TimeAndSale t = null;
	    try{
		    ObjectInputStream is = new ObjectInputStream(in);
		    return (TimeAndSale)is.readObject();
	    }
	    catch(IOException e){
	    	e.printStackTrace();
	    }
	    catch(ClassNotFoundException e){
	    	e.printStackTrace();
	    }
	    return t;
	}

	public byte[] toBytes(TimeAndSale trade) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
		try{
	        ObjectOutputStream o = new ObjectOutputStream(b);
	        o.writeObject(trade);
		}
		catch(IOException e){
			e.printStackTrace();
		}
        return b.toByteArray();
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	public byte[] serialize(String arg0, TimeAndSale trade) {
		System.out.println("BYTES ENCODED");
        ByteArrayOutputStream b = new ByteArrayOutputStream();
		try{
	        ObjectOutputStream o = new ObjectOutputStream(b);
	        o.writeObject(trade);
		}
		catch(IOException e){
			e.printStackTrace();
		}
        return b.toByteArray();
	}

}
