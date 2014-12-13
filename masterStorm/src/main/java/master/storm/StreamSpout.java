package master.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StreamSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	String dirIP;
	int port;

	public StreamSpout(String dirIP, int port) {
		this.dirIP = dirIP;
		this.port = port;
	}
	
	private boolean tweetValido(String tweet){
		boolean resul = false;
		Map mapTweet, mapEntities;
		ObjectMapper twitterParser = new ObjectMapper();
		try {
			mapTweet = twitterParser.readValue(tweet, Map.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			System.out.println("********************************ERROR AL PARSEAR TWEET");
			return false;
		}
		if (mapTweet.containsKey("entities") && mapTweet.containsKey("place") && mapTweet.containsKey("timestamp_ms")) {
			mapEntities = (Map) mapTweet.get("entities");
			ArrayList<Map> hashtags = (ArrayList<Map>) mapEntities
					.get("hashtags");
			if (hashtags.isEmpty()) {
				return false;
			}
			resul = true;
		}
		return resul;
	}
	private String getTweet(){
		String tweet = new String();
		Socket socket;
		try {
			socket = new Socket(this.dirIP, this.port);			
			InputStream inputStream = socket.getInputStream();
			InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			
			tweet =  bufferedReader.readLine();
			socket.close();
		} catch (IOException e) {
			System.out.println("-----------------------ERROR EN STREAM SPOUT - GET TWEET");

			try {
				TimeUnit.MILLISECONDS.sleep(5000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return null;
		}
		
		return tweet;
	}
	
	public void nextTuple() {
		String tweet = this.getTweet();
		if(tweet!=null){
			if(this.tweetValido(tweet)){
				System.out.print("*");
				this.collector.emit(new Values(tweet));
			} else
				System.out.print("-");
		}
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		this.collector = arg2;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}
