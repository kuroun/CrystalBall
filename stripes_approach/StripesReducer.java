package stripes_approach;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StripesReducer extends MapReduceBase implements Reducer<Text, MapWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<MapWritable> values,
			OutputCollector<Text,Text> output, Reporter r)
			throws IOException {
		MapWritable result = new MapWritable();
		int maginal = 0;
		
		while(values.hasNext()){
			MapWritable each = values.next();
			//convert each mapwritable to entry set to loop through each element
			for(Entry<Writable, Writable> entry : each.entrySet()){
				Text k = (Text) entry.getKey();
				IntWritable v = (IntWritable) entry.getValue();
				if(result.containsKey(k)){
					IntWritable oldVal = (IntWritable) result.get(k);
					int newVal = oldVal.get() + v.get();
					result.put(k,new IntWritable(newVal));
				}
				else{
					result.put(k, v);
				}
				maginal += v.get();
			}
		}
		
		for(Entry<Writable, Writable> entry : result.entrySet()){
			Text k = (Text) entry.getKey();
			IntWritable v = (IntWritable) entry.getValue();
			double newVal = ((double)v.get()/maginal);
			result.put(k, new DoubleWritable(newVal));			
		}
		String stripes = "[";
		for(Entry<Writable, Writable> entry : result.entrySet()){
			Text k = (Text) entry.getKey();
			DoubleWritable v = (DoubleWritable) entry.getValue();
			DecimalFormat df = new DecimalFormat("#.##");
			stripes += "(" + k.toString() + "," + df.format(v.get()) + "),";
		}
		stripes += "]";
		//System.out.println(key + "," + stripes);
		output.collect(key, new Text(stripes));
	}
	
}
