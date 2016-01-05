package hybrid_approach;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverClass extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Please give proper input and output directories");
			return -1;
		}
		JobConf conf = new JobConf(DriverClass.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(PairMapper.class);
		conf.setReducerClass(StripesReducer.class);

		conf.setMapOutputKeyClass(Pair.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String args[]) throws Exception {	
		int exitCode = ToolRunner.run(new DriverClass(), args);
		System.exit(exitCode);
	}

}