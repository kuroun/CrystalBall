package stripes_approach;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StripesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, MapWritable>{

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, MapWritable> output, Reporter r)
			throws IOException {
		String s = value.toString();
		String[] terms = s.split(" ");
		for(int i = 0; i< terms.length-1; i++){
			if(terms[i].length()>0){
				output.collect(new Text(terms[i]), neighborhood(terms[i], i, terms));
			}
		}
		
	}
	private MapWritable neighborhood(String term, int index, String[] terms){
		MapWritable n = new MapWritable();
		int i = index + 1;
		while(!term.equals(terms[i])){
			Text word = new Text(terms[i]);
			if(n.containsKey(word)){
				IntWritable value = (IntWritable) n.get(word);
				int count = value.get();
				count += 1;
				n.put(word, new IntWritable(count));
			}
			else{
				n.put(word, new IntWritable(1));
			}
			i++;
			if(i==terms.length) break;
			
		}
		return n;
	}
	


}
