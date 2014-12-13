package master.storm;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.joda.time.DateTime;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReduceBolt extends BaseRichBolt {
	private ArrayList<String> idiomas;

	public ReduceBolt(ArrayList<String> idiomas) {
		this.idiomas = idiomas;
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}

	public void execute(Tuple input) {
		Vector<Tuple> lista = (Vector<Tuple>) input.getValueByField("palabras");
		this.reduce(lista);
	}

	private void escribirEnFichero(ArrayList<String> topK) {
		System.out.println("*************************");
		try {
			Writer writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("topKWords.log", true)));
			String linea = new String();
			for (String string : topK) {
				linea += string + ",";
			}
			writer.write((linea.substring(0, linea.length() - 1)) + "\n");
			writer.close();
			System.out.println(linea.substring(0, linea.length() - 1));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("*************************");
	}

	private String generarTopK(Map<String, Integer> lista, int k, String pais) {
		String salida = new String("[" + pais);
		int count = 0;
		for (Map.Entry<String, Integer> entry : lista.entrySet()) {
			count++;
			salida += " , " + entry.getKey()+" ("+entry.getValue()+")";
			if (count >= k)
				break;
		}
		for (int i = count; i < k; i++)
			salida += ",";
		salida += "]";
		return salida;
	}

	private void reduce(Vector<Tuple> lista) {
		ArrayList<String> topK = new ArrayList<String>();
		for (String idioma : idiomas) {
			Map<String, Integer> counts = new HashMap<String, Integer>();
			for (Tuple tuple : lista) {
				if (tuple.getStringByField("pais").equals(idioma)) {
					String palabra = tuple.getStringByField("palabra");
					Integer count = counts.get(palabra);
					if (count == null)
						count = 0;
					count++;
					counts.put(palabra, count);
				}
			}
			topK.add(this.generarTopK(sortByComparator(counts), 3, idioma));
		}		
		this.escribirEnFichero(topK);
	}

	private Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap) {

		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(
				unsortMap.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it
				.hasNext();) {
			Map.Entry<String, Integer> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
