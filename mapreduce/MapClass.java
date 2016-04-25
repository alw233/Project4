package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private int keyColumn = 1;
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		Text outputVal = new Text(tokenizer.nextToken());
		
		for (int i = 1; i < keyColumn; i++) {
			tokenizer.nextToken();
		}
		
		Text outputKey = new Text(tokenizer.nextToken());
		
		output.collect(outputKey, outputVal);
	}
	
	public void configure(JobConf job) {
		keyColumn = Integer.parseInt(job.get("column"));
	}
}
