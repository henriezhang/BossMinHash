/*功能：
 *作者：henriezhang
 *时间：20131011
 */
package com.webdev.tubd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GroupUrlJoin {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.out.println("AAA");
        /*if(true) {
            throw new RuntimeException(conf.get("fs.default.name"));
        }*/
        System.out.println(conf.get("fs.default.name"));
        System.out.println("BBB");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //String[] otherArgs = args;
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 5) {
            System.err.println("Usage: hadoop jar BossGroupUrlJoin.jar com.webdev.tubd.GroupUrlJoin <group_in> <url_in> <out> <queue> <reduce num>");
            System.exit(5);
        }

        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", otherArgs[3]);
        conf.set("mapred.queue.name", otherArgs[3]);
        Job job = new Job(conf, "BossGroupUrlJoin");
        job.setJarByClass(GroupUrlJoin.class);
        job.setMapperClass(GroupUrlJoinMapper.class);
        job.setReducerClass(GroupUrlJoinReducer.class);
        job.setNumReduceTasks(Integer.valueOf(otherArgs[4]).intValue());

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Record.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set input and output
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        System.out.println(otherArgs[0]);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        System.out.println(otherArgs[1]);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.out.println(otherArgs[2]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // 数据记录
    public static class Record implements Writable {
        int type; //数据类型的定义,1:qq->group,2qq->url
        String qq = "";
        String gId = "";
        String url = "";

        public Record() {
            super();
        }

        public Record(Record record) {
            this.qq = record.qq;
            this.gId = record.gId;
            this.url = record.url;
        }

        public String toString() {
            if (type == 1) {
                return qq + "," + gId;
            } else if (type == 2) {
                return qq + "," + url;
            }
            return "uninit data!";
        }

        public void readFields(DataInput in) throws IOException {
            type = in.readInt();
            gId = in.readUTF();
            url = in.readUTF();
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(type);
            out.writeUTF(gId);
            out.writeUTF(url);
        }
    }

    public static class GroupUrlJoinMapper
            extends Mapper<LongWritable, Text, Text, Record> {
        private Pattern pUrl;
        private Matcher mUrl;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            pUrl = Pattern.compile("^/a/201\\d[^_]+?\\.htm$");
        }

        /*
         * map:取出收中的qq和对应的url,qq对应的group
         */
        @Override
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t|,");
            Record reco = new Record();
            if (fields.length == 19) {
                mUrl = pUrl.matcher(fields[5]);
                if (mUrl.find()) {
                    reco.type = 2;
                    reco.qq = fields[17];
                    reco.url = fields[4] + fields[5];
                } else {
                    return;
                }
            } else if (fields.length == 2) {
                reco.type = 1;
                reco.gId = fields[0];
                reco.qq = fields[1];
            } else {
                return;
            }
            context.write(new Text(reco.qq), reco);
        }
    }

    /*
     * reduce: 取最小的前n个hash值 作为用户所在的类
     */
    public static class GroupUrlJoinReducer
            extends Reducer<Text, Record, Text, Text> {
        @Override
        public void reduce(Text uin, Iterable<Record> recordList, Context context)
                throws IOException, InterruptedException {
            Record group = new Record();
            List<Record> urls = new Vector<Record>();
            for (Record reco : recordList) {
                if (reco.type == 1) { //1 is group
                    group = new Record(reco);
                } else {  //2 is urls
                    Record recoClone = new Record(reco);
                    urls.add(recoClone);
                }
            }

            if (group.gId != "") {
                for (Record e : urls) {
                    context.write(new Text(group.gId), new Text(e.url));
                }
            }
        }
    }
}