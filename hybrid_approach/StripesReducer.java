package hybrid_approach;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StripesReducer extends MapReduceBase implements Reducer<Pair, IntWritable, Text, Text> {
	
	
	private Text current_term = null ;
	private int maginal = 0;
	private HashMap<String, Double> stripes = new HashMap<String, Double>();
	
	@Override
	public void reduce(Pair key, Iterator<IntWritable> values,
			OutputCollector<Text,Text> output, Reporter r)
			throws IOException {
		Text w = key.getElement();
		Text u = key.getNeighbour();
		if(current_term == null){
			current_term = w;
			
		}
		else if(!current_term.equals(w)){
				updateStripes(stripes, maginal);
				System.out.println(current_term + ", " + stripesToText(stripes));
				output.collect(current_term, stripesToText(stripes));
				stripes.clear();
				maginal = 0;
				
		}	
		int sum = sumStripe(values);
		maginal += sum;
		stripes.put(u.toString(),(double)sum);	
		current_term = w;
	}
	
	private int sumStripe(Iterator<IntWritable> values){
		int sum = 0;
		while(values.hasNext()){
			sum += values.next().get();
		}
		return sum;
	}
	private void updateStripes(HashMap<String, Double> stripes, int maginal){
		for(Entry<String, Double> entry : stripes.entrySet()){
			double newValInt = entry.getValue();
			double frequency = newValInt/maginal;
			stripes.put(entry.getKey(), frequency);
		}
	}

	private Text stripesToText(HashMap<String, Double> result){
		String stripes = "[";
		for(Entry<String, Double> entry : result.entrySet()){
			String k = entry.getKey();
			double v = entry.getValue();
			DecimalFormat df = new DecimalFormat("#.##");
			stripes += "(" + k + "," + df.format(v) + "),";
		}
		stripes += "]";
		return new Text(stripes);
	}
	
}
