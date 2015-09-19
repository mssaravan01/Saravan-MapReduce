# Saravan-MapReduce
Saravana Kumar Babu Surendran Repository

1.To find frequencies of letters [a-z] in a huge text of few GBs, do you need Map/Reduce?

-----------------------------------------------
Mapper
-----

package cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CCMap extends Mapper<Object, Text, Text, LongWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    /*
     * TODO implement
     */
	  String[] words = value.toString().split("[ \t]+");
	  for(String word:words)
	  {		 
		  word=word.replaceAll("[^a-zA-Z0-9]", "");
		  int l=word.length();
		 char ch;
		  for(int j=0; j<l; j++){ 
			  ch=word.charAt(j); 
		  context.write(new Text(Character.toString(ch)), new LongWritable(1));
		  }
	  }
  }
}

Reducer
--------
package cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
	  
	  long sum = 0;
	  for(LongWritable iw:values)
	  {
		  sum += iw.get();
	  }
	  context.write(key, new LongWritable(sum));
  }
}

Driver
--------
package cc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CCDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		JobConf conf = new JobConf();
		Job job = new Job(conf, "cccount");
		job.setJarByClass(CCDriver.class);
		
		job.setMapperClass(CCMap.class);
		job.setReducerClass(CCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}

