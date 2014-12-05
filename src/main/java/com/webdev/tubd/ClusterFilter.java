/*功能：过滤出聚类成员数比较少的用户    
 *作者：henriezhang
 *时间：20131011
 */
package com.webdev.tubd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;
import java.util.ArrayList;

public class ClusterFilter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //String[] otherArgs = args;
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 4) {
            System.err.println("Usage: hadoop jar WbRetweetTrain.jar com.webdev.boss.WbRetweet <in> <out> <queue> <reduce num>");
            System.exit(4);
        }

        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", otherArgs[2]);
        conf.set("mapred.queue.name", otherArgs[2]);
        Job job = new Job(conf, "BossMhClusterFilter");
        job.setJarByClass(ClusterFilter.class);
        job.setMapperClass(ClusterFilterMapper.class);
        job.setReducerClass(ClusterFilterReducer.class);
        job.setNumReduceTasks(Integer.valueOf(otherArgs[3]).intValue());

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is Text, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set input and output
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class ClusterFilterMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        /*
         * map:取出收中的gid和对应的qq
         */
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t", 2);
            if (fields.length < 2) {
                return;
            }

            context.write(new Text(fields[0]), new Text(fields[1]));
        }
    }

    /*
     * reduce: 将聚类成员小于10个的过滤掉
     */
    public static class ClusterFilterReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text gid, Iterable<Text> qqValues, Context context)
                throws IOException, InterruptedException {
            int i = 0;
            List<Text> qqList = new ArrayList<Text>();
            for (Text val : qqValues) {
                i++;
                qqList.add(new Text(val));
            }

            // 只保留成员个数大于N个的聚类
            int minNum = 30;
            if (i >= minNum) {
                for (Text val : qqList) {
                    context.write(gid, val);
                }
            }
        }
    }
}