package master.storm;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import backtype.storm.tuple.Tuple;

public class TwitterWindow {

	private long windowSize;
	private long advance;
	private String timestampFieldName;

	private long upperLimitTS;
	private long lowerLimitTS;

	private TreeMap<Long, Vector<Tuple>> window, mirror_window;

	public TwitterWindow(long windowSize, long advance,
			String timestampFieldName) {
		this.windowSize = windowSize;
		this.advance = advance;
		this.timestampFieldName = timestampFieldName;
		this.window = new TreeMap<Long, Vector<Tuple>>();
		this.mirror_window = new TreeMap<Long, Vector<Tuple>>();
		this.upperLimitTS = windowSize - 1;
		this.lowerLimitTS = 0;
	}

	public Vector<Tuple> getTuplesInWin() {
		Vector<Tuple> tuplesInWin = new Vector<Tuple>();
		for (long groupTSKey : this.window.keySet()) {
			for (Tuple tuple : this.window.get(groupTSKey)) {
				tuplesInWin.add(tuple);
			}
		}
		return tuplesInWin;
	}
	
	public Vector<Tuple> addTuple(Tuple tuple) {
		long timestamp = Long.parseLong(tuple
				.getStringByField(this.timestampFieldName));
		//System.out.println("Processing tuple with timestamp "+timestamp);
		if (timestamp < this.lowerLimitTS) {
			return null;
		} else {
			Vector<Tuple> windowedTuples = null;
			if(timestamp>this.upperLimitTS){
				windowedTuples = this.getTuplesInWin();
				if (timestamp > (this.upperLimitTS+this.advance)) {
					
					this.upperLimitTS = timestamp;
				} else {
					this.upperLimitTS = this.upperLimitTS+this.advance;
				}
				if((this.upperLimitTS-this.lowerLimitTS)>(this.windowSize-1)){
					this.mirror_window.clear();
					this.lowerLimitTS = timestamp - this.windowSize + this.advance;
					SortedMap<Long,Vector<Tuple>> tailMap = this.window.tailMap(this.lowerLimitTS);
					this.mirror_window.putAll(tailMap);
					this.window.clear();
					this.window.putAll(this.mirror_window);
					System.out.println("WS:"+this.windowSize);
					System.out.println("TIM-"+timestamp);
					System.out.println("LOW-"+this.lowerLimitTS);
					System.out.println("UPP-"+this.upperLimitTS);
				}
			}
			Vector<Tuple> existing_tuples = this.window.get(timestamp);
			if(existing_tuples == null){
				existing_tuples = new Vector<Tuple>();
				existing_tuples.add(tuple);
				this.window.put(timestamp, existing_tuples);
			} else {
				existing_tuples.add(tuple);
			}
			return windowedTuples;
		}

	}
}