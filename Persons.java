import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

import java.io.IOException;
import java.util.regex.Pattern;

public class Persons extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Persons.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Persons(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Persons");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PersonMapper.class);
        job.setCombinerClass(PersonCombiner.class);
       job.setReducerClass(PersonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumJob.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class SumJob implements WritableComparable<SumJob> {

        IntWritable actor;
        IntWritable director;

        public SumJob() {
            set(new IntWritable(0), new IntWritable(0));
        }

        public SumJob(Integer actor, Integer director) {
            set(new IntWritable(actor), new IntWritable(director));
        }

        public void set(IntWritable actor, IntWritable director) {
            this.actor = actor;
            this.director = director;
        }

        public IntWritable getActor() {
            return actor;
        }

        public IntWritable getDirector() {
            return director;
        }

        public void addSumCount(SumJob sumCount) {
           set(new IntWritable(this.actor.get() + sumCount.getActor().get()), new IntWritable(this.director.get() + sumCount.getDirector().get()));

        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

            actor.write(dataOutput);
            director.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

            actor.readFields(dataInput);
            director.readFields(dataInput);
        }

        @Override
        public int compareTo(SumJob sumCount) {

            // compares the first of the two values
            int comparison = actor.compareTo(sumCount.actor);

            // if they're not equal, return the value of compareTo between the "actor" value
            if (comparison != 0) {
                return comparison;
            }

            // else return the value of compareTo between the "director" value
            return director.compareTo(sumCount.director);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SumJob sumCount = (SumJob) o;

            return director.equals(sumCount.director) && actor.equals(sumCount.actor);
        }

        @Override
        public int hashCode() {
            int result = actor.hashCode();
            result = 31 * result + director.hashCode();
            return result;
        }


        @Override
        public String toString() {
            return "Wynik:{" +
                    "aktor=" + actor +
                    ", rezyser=" + director +
                    '}';
        }
    }

    public static class PersonMapper extends Mapper<LongWritable, Text, Text, SumJob> {
        private Text id = new Text();
        private static SumJob job = new SumJob();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            int i = 0;

            for (String token : line.split("\t")) {

                if (i == 2) {
                    id.set(token);
                }
                if (i == 3) {
                    if (token.equals("director")) {
                        job.set(new IntWritable(0), new IntWritable(1));
                        context.write(id, job);
                    }
                    if (token.equals("actor") || token.equals("actress")) {
                        job.set(new IntWritable(1), new IntWritable(0));
                        context.write(id, job);
                    }

                }

                i++;

            }


        }
    }




public static class PersonCombiner extends
        Reducer<Text,SumJob,Text,SumJob>{

        @Override
        public void reduce(Text key,Iterable<SumJob> values, Context context)
                throws IOException, InterruptedException{

            SumJob job = new SumJob (0,0);

            for (SumJob val: values){
                job.addSumCount(val);
            }
            context.write(key,job);
        }
}

    public static class PersonReducer extends
            Reducer<Text,SumJob,Text,Text> {

        private Text result = new Text();
        SumJob job = new SumJob(0,0);
        String counter ="";

        @Override
        public void reduce(Text key,Iterable<SumJob> values, Context context)
            throws IOException, InterruptedException{
            Text id = new Text(key);
            job.set(new IntWritable(0),new IntWritable(0));
            for (SumJob val: values){
                job.addSumCount(val);
            }

          counter  = job.toString();

            result.set(counter);
            context.write(id,result);
        }

    }

}