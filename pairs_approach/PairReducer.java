package pair;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PairReducer extends MapReduceBase implements
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	int total = 0;

	@Override
	public void reduce(Pair key, Iterator<IntWritable> values,
			OutputCollector<Pair, DoubleWritable> output, Reporter r)
			throws IOException {
		if (key.getNeighbour().toString().equals("*")) {
			total = 0;
			while (values.hasNext()) {
				IntWritable i = values.next();
				total += i.get();
			}
		} else {
			int sum = 0;
			while (values.hasNext()) {
				IntWritable i = values.next();
				sum += i.get();
			}
			output.collect(key, new DoubleWritable(sum / (double) total));
		}
	}
}
