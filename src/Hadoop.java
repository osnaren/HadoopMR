import java.io.*;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hadoop {
    public static final Log log = LogFactory.getLog(Hadoop.class);
    public static int ch, sch, mrk, mrk1;
    public static char sec, alp, gnd;
    public static String gender, bgrp, std;

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            String[] data;
            String roll = "";

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String temp = word.toString();
                data = temp.split("\t");
                roll = data[0];
                context.write(new Text(roll), word);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        private MapMR mr = new MapMR();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            switch ((ch * 10) + sch) {
                case 11:
                    mr.playdb_s1(key, values, context);
                    break;
                case 12:
                    mr.playdb_s2(key, values, context);
                    break;
                case 13:
                    mr.playdb_s3(key, values, context);
                    break;
                case 14:
                    mr.playdb_s4(key, values, context);
                    break;
                case 21:
                    mr.bgrp_s1(key, values, context);
                    break;
                case 22:
                    mr.bgrp_s2(key, values, context);
                    break;
                case 31:
                    mr.mark_s1(key, values, context);
                    break;
                case 32:
                    mr.mark_s1(key, values, context);
                    break;
                case 33:
                    mr.mark_s1(key, values, context);
                    break;

            }
        }
    }

    public static void start_mr(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce");
        job.setJarByClass(Hadoop.class);
        job.setMapperClass(Hadoop.TokenizerMapper.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(Hadoop.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        FileSystem.get(conf).delete(new Path(out), true);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Choices cc;
        Configuration config = new Configuration();
        Job job = null;

        String Menu = "\t\t\t---MAIN MENU---\n1)Play with DB\t2)Blood Groups\t3)Marks\t4)Exit\n";
        Scanner s = new Scanner(System.in);


        while (true) {

            System.out.print(Menu);
            System.out.print("Enter Choice : ");
            int n = s.nextInt();
            ch = n;
            switch (n) {
                case 1:
                    cc = new Choices();
                    cc.playdb();
                    start_mr(args[0], args[1] + "\\PlayDB");
                    break;
                case 2:
                    cc = new Choices();
                    cc.bgrp();
                    start_mr(args[0], args[1] + "\\Blood");
                    break;
                case 3:
                    cc = new Choices();
                    cc.marks();
                    start_mr(args[0], args[1] + "\\Mark");
                    break;
                case 4:
                    System.out.println("\t\t\t!---Exiting---!\n");
                    System.exit(1);
                default:
                    System.out.println("Try Other Choice!");
                    break;
            }
        }
    }
}