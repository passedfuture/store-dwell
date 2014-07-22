package com.dwell;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.Text;

public class StoreDwellDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        long startTime, endTime, updateTime;
        Date now = new Date(System.currentTimeMillis());
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
        // 使用Calendar类来操作时间
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(now.getTime());
        long nowTime = c.getTimeInMillis();

        endTime = nowTime / (5 * 60 * 1000) * (5 * 60 * 1000) - 1; // 本次统计数据的结束时间，即距离此刻最近的五分钟的结束时间
        startTime = endTime / (5 * 60 * 1000) * (5 * 60 * 1000); // 本次统计数据的开始时间
                                                                 // ，即最近五分钟的开始时间，数据采用增量的方法进行统计

        updateTime = 0l;
        try {
            updateTime = sdfDate.parse(sdfDate.format(new Date(endTime)))
                    .getTime();
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        Configuration con = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(con, args)
                .getRemainingArgs();

        con.set("hbase.zookeeper.quorum",
                "hadoop2.palmaplus.com, hadoop3.palmaplus.com, hadoop4.palmaplus.com");
        con.setLong("create_time", endTime);
        con.setInt("time_range", Integer.parseInt(otherArgs[1]));
        con.setLong("update_time", updateTime);

        Job job = new Job(con, "store_dwell");
        job.setJarByClass(StoreDwellDriver.class);

        Scan scan = new Scan();
        scan.setCaching(3000);
        scan.setCacheBlocks(false);
        scan.setTimeRange(startTime, endTime);
        scan.setMaxVersions();
        scan.addColumn(Bytes.toBytes("RSSI"), Bytes.toBytes("RSSI"));
        TableMapReduceUtil.initTableMapperJob("RSSI", scan, MapClass.class,
                Text.class, LongArrayWritable.class, job);

        // TableMapReduceUtil.addDependencyJars(job);
        // job.setReducerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(BSONWritable.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        // job.setOutputFormatClass(MongoOutputFormat.class);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new StoreDwellDriver(),
                args);
        System.exit(res);
    }

}
