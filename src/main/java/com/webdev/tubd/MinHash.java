/*功能：MinHash聚类算法  
 *作者：henriezhang
 *时间：20131011
 */
package com.webdev.tubd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.Date;
import java.util.*;
import java.text.*; 
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MinHash 
{
    public static class MinHashMapper 
        extends Mapper<LongWritable, Text, Text, Text>
    {
        private final static Text urlValue = new Text("");
        private Pattern pUrl;
        private Matcher mUrl;
        private Pattern pUin;
        private Matcher mUin;

        public void setup(Context context)throws IOException,InterruptedException
        {
            super.setup(context);
            pUrl = Pattern.compile("^/a/201\\d[^_]+?\\.htm$");
            pUin = Pattern.compile("^[1-9][0-9]{4,11}$");
        }

        /*
         * map:取出收中的qq和对应的url
         */
        public void map(LongWritable key, Text inValue, Context context) 
            throws IOException, InterruptedException 
        {
            String[] fields = inValue.toString().split(",", 19);
            if(fields.length < 18) {
                return ;
            }

            mUrl = pUrl.matcher(fields[5]);
            mUin = pUin.matcher(fields[17]);
            if(mUrl.find() && mUin.find()) {
                urlValue.set(fields[4]+fields[5]);
                context.write(new Text(fields[17]), urlValue);
            }
        }
    }

    /*
     * reduce: 取最小的前n个hash值 作为用户所在的类  
     */
    public static class MinHashReducer 
        extends Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text uin, Iterable<Text> urlValues, Context context) 
            throws IOException, InterruptedException 
        {
            TreeSet<Object> urlSet = new TreeSet<Object>();  
            for (Text val : urlValues) {
                urlSet.add(val.toString().hashCode());
            }

            int maxNum = 3;
            if(urlSet.size()>=maxNum) {
                int i = 0;
                String res = "";
                Iterator iter = urlSet.iterator();
                while(iter.hasNext() && i++<maxNum) {
                    res += "_" + String.valueOf(iter.next());
                }
                context.write(new Text(""+res.hashCode()), uin);
            }
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //String[] otherArgs = args;
        System.err.println(otherArgs.length+":"+otherArgs.toString());
        if (otherArgs.length < 4) {
            System.err.println("Usage: hadoop jar BossMinHash.jar com.webdev.tubd.MinHash <in> <out> <queue> <reduce num>");
            System.exit(4);
        }

        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", otherArgs[2]);
        conf.set("mapred.queue.name", otherArgs[2]);
        Job job = new Job(conf, "BossMinHash");
        job.setJarByClass(MinHash.class);
        job.setMapperClass(MinHashMapper.class);
        job.setReducerClass(MinHashReducer.class);
        job.setNumReduceTasks(Integer.valueOf(otherArgs[3]).intValue());

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        ParsePosition pos = new ParsePosition(0);
        Date dt = formatter.parse(otherArgs[4], pos);
        Calendar cd = Calendar.getInstance();
        cd.setTime(dt);
        FileSystem fs = FileSystem.get(conf);
        for (int i = 0; i < 2; i++) {
            String tmpPath = otherArgs[0] + "/ds=" + formatter.format(cd.getTime());
            Path tPath = new Path(tmpPath);
            if (fs.exists(tPath)) {
                FileInputFormat.addInputPath(job, tPath);
                System.out.println("Exist " + tmpPath);
            } else {
                System.out.println("Not exist " + tmpPath);
            }
            cd.add(Calendar.DATE, -1);
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}