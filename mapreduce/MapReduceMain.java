package mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceMain extends Configured implements Tool {

	public int run(String[] args) throws IOException, URISyntaxException {
		JobConf conf = new JobConf(MapReduceMain.class);
		//DistributedCache.addCacheFile(new File("/Users/angelayang316/Downloads/Dataset.txt").toURI(), conf);
		conf.set("column", args[0]);
		conf.setJobName("mapreduce");
		conf.setJarByClass(MapReduceMain.class);
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		conf.setMapOutputKeyClass(Text.class);
		FileInputFormat.setInputPaths(conf, new Path("/In/Dataset.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("/Out"));
		conf.setOutputKeyClass(Text.class); //out keys are one column in file
		conf.setOutputValueClass(Text.class); //values are clothing names
		
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), new MapReduceMain(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
        System.exit(res);
	}
}
