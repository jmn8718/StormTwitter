package master.storm;

import java.util.ArrayList;
import java.util.Map;
import java.util.Vector;

import org.joda.time.DateTime;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MapBolt extends BaseRichBolt {
	private OutputCollector collector;
	private TwitterWindow window;
	private long windowSize, advance;

	public MapBolt(long windowSize,long advance) {
		this.windowSize = windowSize;
		this.advance = advance;
	}

	public void execute(Tuple input) {
		Vector<Tuple> windTuples = this.window.addTuple(input);
		if (windTuples != null && !windTuples.isEmpty()) {
			//System.out.println("//////////////////////////////////Size: "+windTuples.size());
			this.collector.emit(new Values(windTuples));
		}

	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		this.window = new TwitterWindow(this.windowSize,this.advance,
				"timestamp");
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("palabras"));
	}
}
