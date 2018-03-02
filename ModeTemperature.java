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

public class ModeTemperature extends Configured implements Tool {


  public static class ModeTemperatureMapper
      extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    private Record avgTemp = null;

    public void map(LongWritable k, Text v, Context context) {
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

    }catch(Exception e){
      e.getMessage();
    }
}
}



  public static class ModeTemperatureReducer
      extends Reducer<LongWritable, LongWritable, LongWritable, NullWritable> {

    public void reduce(LongWritable k, Iterable<LongWritable> list, Context context)
        throws IOException, InterruptedException {


      long count = 0;

      HashMap<Long,Long> map = new HashMap<>();
      for(LongWritable i:list){
        if(!map.containsKey(i.get())){
          map.put(i.get(), count);
        }
        else{
          map.put(i.get(), map.get(i.get())+1);
        }

      }
      long maxcount=0;
      long mode=0;
      for(Long i:map.keySet()){

          if(map.get(i)>maxcount)
          {
            maxcount=map.get(i);
            mode=i;
          }

      }
      context.write(new LongWritable(mode), NullWritable.get());
    }
  }

  public int run(String [] args) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(ModeTemperature.class);

    job.setMapperClass(ModeTemperatureMapper.class);
    job.setReducerClass(ModeTemperatureReducer.class);

    FileInputFormat.addInputPath(job, new Path("records3")); //input directory of HDFS
    FileOutputFormat.setOutputPath(job, new Path("avgtemp3")); //output directory

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      return 1; // error.
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new ModeTemperature(), args);
    System.exit(ret);
  }
}
