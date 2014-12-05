/*功能：GroupNewsRank聚类算法  
 *作者：henriezhang
 *时间：20131011
 */
package com.webdev.tubd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class GroupNewsRank {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 4) {
            System.err.println("Usage: hadoop jar BossGroupNewsRank.jar com.webdev.tubd.GroupNewsRank <in> <out> <queue> <reduce num>");
            System.exit(4);
        }

        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", otherArgs[2]);
        conf.set("mapred.queue.name", otherArgs[2]);
        Job job = new Job(conf, "BossGroupNewsRank");
        job.setJarByClass(GroupNewsRank.class);
        job.setMapperClass(GroupNewsRankMapper.class);
        job.setReducerClass(GroupNewsRankReducer.class);
        job.setNumReduceTasks(Integer.valueOf(otherArgs[3]).intValue());

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set input and output
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // 新闻项
    public static class News {
        String url = "";
        int pubtime = 0;
        int pv = 0;

        public News() {

        }

        public News(String url, int pubtime, int pv) {
            this.url = url;
            this.pubtime = pubtime;
            this.pv = pv;
        }
    }

    public static class GroupNewsRankMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        /*
         * map:取出收中的qq和对应的url
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
     * reduce: 取最小的前n个hash值 作为用户所在的类
     */
    public static class GroupNewsRankReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text gid, Iterable<Text> urlValues, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> urlMap = new HashMap<String, Integer>();
            for (Text val : urlValues) {
                String key = val.toString();
                int value = 1;
                if (urlMap.containsKey(key)) {
                    value = (int) urlMap.get(key) + 1;
                }
                urlMap.put(key, value);
            }

            // 按照流量进行排序然后输出top-50
            List<Map.Entry<String, Integer>> urlList =
                    new LinkedList<Map.Entry<String, Integer>>(urlMap.entrySet());
            // 排序
            Collections.sort(urlList, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return (o2.getValue() - o1.getValue());
                }
            });
            // 输出排序结果
            StringBuilder sb = new StringBuilder();
            for(int i=0; i<urlList.size() && i<50; i++) {
                sb.append(",");
                sb.append(urlList.get(i).getKey());
                sb.append(":");
                sb.append(urlList.get(i).getValue());
            }
            context.write(gid, new Text(sb.substring(1)));

            /*Iterator hit = urlMap.entrySet().iterator();
            StringBuilder sBuilder = new StringBuilder("");
            while (hit.hasNext()) {
                Map.Entry entry = (Map.Entry) hit.next();
                sBuilder.append(",");
                sBuilder.append(entry.getKey().toString());
                sBuilder.append(":");
                sBuilder.append(entry.getValue().toString());
            } // end while
            sBuilder.deleteCharAt(0);
            context.write(gid, new Text(sBuilder.toString()));
            */
        }
    }
}