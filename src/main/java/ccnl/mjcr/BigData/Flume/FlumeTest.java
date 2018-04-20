package ccnl.mjcr.BigData.Flume;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

public class FlumeTest {

	public static void main(String[] args) {
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String date = "";
		MyRpcClient client = new MyRpcClient();
		client.init("192.168.0.2", 34443);
		
		String sampleData = "1111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000";
		//String sampleData = "1234567890";
		
		client.sendDataToFlume(sampleData);
		date = df.format(new Date());
		System.out.println("start time: "+date);
		
		for(int i =0;i<1048575;i++)
		{
			client.sendDataToFlume(sampleData);
		}
		
		date = df.format(new Date());
		System.out.println("end time:"+date);
		client.cleanUp();
	}

}

class MyRpcClient {
	private RpcClient client;
	private String hostname;
	private int port;
	
	public void init(String hostname,int port) {
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
	}
	
	public void sendDataToFlume(String data) {
		Event event = EventBuilder.withBody(data,Charset.forName("UTF-8"));
		try {
			client.append(event);
		} catch(EventDeliveryException e) {
			client.close();
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
		}
	}
	
	public void cleanUp() {
		client.close();
	}
}
