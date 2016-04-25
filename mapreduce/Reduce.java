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
	
	String[] searchCriteria;
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String outputValue = "";
		while (values.hasNext()) {
			outputValue += " ";
			outputValue += values.next().toString();
		}
		
		if (searchCriteria[0].matches("none")) {
			output.collect(key,  new Text(outputValue));
		}
		else {
			for (int i = 0; i < searchCriteria.length; i++) {
				if (searchCriteria[i].matches(key.toString())) {
					output.collect(key, new Text(outputValue));
					break;
				}
				else {
					System.out.println(key.toString() + " is not " + searchCriteria[i]);
				}
			}
		}
	}

	public void configure(JobConf job) {
		/*try {
		Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		searchCriteria = job.getStrings("criteria");
	}

}
