package master.storm;

import java.util.ArrayList;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class TopKTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout",
				new StreamSpout(args[0], Integer.parseInt(args[1])),2);

		ArrayList<String> paises = new ArrayList<String>();
		String[] pais = args[2].split(",");
		for (String p : pais) {
			paises.add(p);
		}

		builder.setBolt("receiverBolt", new ReceiverBolt(paises), 3)
				.shuffleGrouping("spout");

		builder.setBolt("mapBolt", new MapBolt(600,200), 3)
				.shuffleGrouping("receiverBolt");

		builder.setBolt("reduceBolt", new ReduceBolt(paises),2)
				.shuffleGrouping("mapBolt");

		Config conf = new Config();

		try {
			StormSubmitter.submitTopology("TopKTopology", conf,
					builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
		
		/*LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("TopKTopology", conf,
				builder.createTopology());*/
	}

}
