import java.io.IOException;

import com.cloudera.sqoop.lib.RecordParser.ParseError;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import java.util.*;

public class AvgTemperature extends Configured implements Tool {

  public static class AvgTemperatureMapper
      extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    
    public void map(LongWritable k, Text v, Context context) {
    //Record class has been generated using codegen sqoop command 
      Record record = new Record();
      try {
        record.parse(v); // Auto-generated: parse all fields from text.
      } catch (ParseError pe) {
        // Got a malformed record. Ignore it.
        return;
      }

      try{
      long temperature = record.get_airtemperature();
      if (temperature != 9999) {
          context.write(new LongWritable(0),new LongWritable(temperature));
        }
     } catch(Exception e){
      e.getMessage();
    }
  }
}

  public static class AvgTemperatureReducer
      extends Reducer<LongWritable, LongWritable, FloatWritable, NullWritable> {

    public void reduce(LongWritable k, Iterable<LongWritable> list, Context context)
        throws IOException, InterruptedException {

      long sum = 0;
      long count = 0;
      
      for(LongWritable i:list){
        sum +=i.get();
        count++;
      }
      Float avg = (float)sum/count;
      context.write(new FloatWritable(avg), NullWritable.get());
    }
  }

  public int run(String [] args) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(AvgTemperature.class);

    job.setMapperClass(AvgTemperatureMapper.class);
    job.setReducerClass(AvgTemperatureReducer.class);

    FileInputFormat.addInputPath(job, new Path("records"));
    FileOutputFormat.setOutputPath(job, new Path("avgtemp2"));

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      return 1; // error.
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new AvgTemperature(), args);
    System.exit(ret);
  }
}
