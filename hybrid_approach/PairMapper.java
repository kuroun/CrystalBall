package hybrid_approach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PairMapper extends MapReduceBase implements Mapper<LongWritable, Text, Pair, IntWritable> {

	private HashMap<Pair, Integer> outputMap = new HashMap<>();

	@Override
	public void map(LongWritable key, Text values,
			OutputCollector<Pair, IntWritable> output, Reporter r)
			throws IOException {
		
		String input = values.toString();
		String[] readLines = input.split("//.*\n");
		
		for (String line : readLines) {
			String[] words = line.split("[ ]{1,}");
			for (int i = 0; i < words.length - 1; i++) {
				for (int j = i + 1; j < words.length; j++) {
					if (words[i].equals(words[j]))
						break;

					Pair pair = new Pair(words[i], words[j]);

					if (outputMap.get(pair) != null)
						outputMap.put(pair, outputMap.get(pair) + 1);
					else
						outputMap.put(pair, new Integer(1));
				}
			}
		}

		Iterator<Entry<Pair, Integer>> iterator = outputMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Pair, Integer> mapEntry = iterator.next();
			output.collect(mapEntry.getKey(), new IntWritable(mapEntry.getValue()));
			iterator.remove();
		}
		Pair end = new Pair("{","}");
		output.collect(end, new IntWritable(0));
	}
}