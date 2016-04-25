package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String outputValue = "";
		while (values.hasNext()) {
			outputValue += " ";
			outputValue += values.next().toString();
		}
		output.collect(key, new Text(outputValue));
	}

	public void configure(JobConf job) {
		/*try {
		Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}

}
