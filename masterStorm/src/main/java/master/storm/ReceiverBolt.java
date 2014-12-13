package master.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ReceiverBolt extends BaseRichBolt {
	private ArrayList<String> paises;
	private OutputCollector collector;
	
	public ReceiverBolt(ArrayList<String> paises) {
		this.paises = paises;
	}
	
	private List<Values> parseTweet(String tweet){
		List<Values> lista = new ArrayList<Values>();
		Map mapTweet, mapUser, mapEntities;
		ObjectMapper twitterParser = new ObjectMapper();
		try {
			mapTweet = twitterParser.readValue(tweet, Map.class);
		} catch (Exception e) {
			System.out.println("----------------------ERROR PARSE BOLT RECEIVE");
			mapTweet = null;
		}
		if (this.paises.contains(mapTweet.get("lang")) && mapTweet.containsKey("entities") && mapTweet.containsKey("place")) {
			mapEntities = (Map) mapTweet.get("entities");
			ArrayList<Map> hashtags = (ArrayList<Map>) mapEntities
					.get("hashtags");
			if (!hashtags.isEmpty()) {
				for (Map hashtag : hashtags) {
					Values tupla = new Values();
					mapUser = (Map) mapTweet.get("user");
					tupla.add(mapUser.get("name"));
					tupla.add(mapTweet.get("lang"));
					tupla.add(hashtag.get("text"));
					Long seconds = TimeUnit.MILLISECONDS.toSeconds(Long.valueOf((String) mapTweet.get("timestamp_ms")));
					tupla.add(seconds.toString());
					lista.add(tupla);
				}
			}
		}
		
		
		return lista;
	}
	public void execute(Tuple input) {
		String tweet = input.getStringByField("tweet");
		List<Values> tuplas = this.parseTweet(tweet);
		if(!tuplas.isEmpty())
			for (Values value : tuplas) {
				this.collector.emit(value);
			}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("usuario", "pais", "palabra", "timestamp"));
	}

}
