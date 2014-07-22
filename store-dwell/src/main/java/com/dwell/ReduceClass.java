package com.dwell;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.hadoop.io.BSONWritable;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReduceClass extends
        Reducer<Text, LongArrayWritable, Text, BSONWritable> {
    private MongoClient mongo;
    private DB db;
    private DBCollection dbCollection;
    private long createTime;
    private long updateTime;
    private Map<String, Map<String, long[]>> resMap = new HashMap<String, Map<String, long[]>>();

    public void setup(Context context) throws UnknownHostException {
        mongo = new MongoClient(Arrays.asList(new ServerAddress("10.1.8.45",
                27017), new ServerAddress("10.1.8.46", 27017),
                new ServerAddress("10.1.8.47", 27017)));
        db = mongo.getDB("bi_single_store");
        dbCollection = db.getCollection("record_5min_user_info");
        Configuration conf = context.getConfiguration();
        createTime = conf.getLong("create_time", 0);
        updateTime = conf.getLong("update_time", 0);

    }

    public void reduce(Text key, Iterable<LongArrayWritable> values,
            Context context) throws IOException, InterruptedException {
        String keyArr[] = key.toString().split("_");
        String storeId = keyArr[0];
        String userMac = keyArr[1];
        System.out.println(storeId + "\t" + userMac);
        Set<Writable> timeSet = new HashSet<Writable>();
        for (LongArrayWritable value : values) {
            Writable timeArr[] = value.get();
            System.out.println("数组长度：" + timeArr.length);
            timeSet.addAll(Arrays.asList(timeArr));

        }
        System.out.println("set长度：" + timeSet.size());
        // 把时间戳数据放到数组里
        long timeArr[] = new long[timeSet.size()];
        int i = 0;
        for (Writable time : timeSet) {
            timeArr[i++] = ((LongWritable) time).get();
        }
        System.out.println("整理后数组长度：" + timeArr.length);
        // 将数据放入resMap

        Map<String, long[]> tempResMap = null;
        if (resMap.containsKey(storeId)) {
            tempResMap = resMap.get(storeId);
        } else {
            tempResMap = new HashMap<String, long[]>();
        }
        tempResMap.put(userMac, timeArr);
        resMap.put(storeId, tempResMap);
    }

    public void cleanup(Context context) {
        int mapSize = resMap.size();
        String keyArr[] = resMap.keySet().toArray(new String[mapSize]);
        for (int i = 0; i < mapSize; i++) {
            String storeId = keyArr[i];
            Map<String, long[]> dataMap = resMap.get(storeId);
            int userMacSize = dataMap.size();
            String userMacArr[] = dataMap.keySet().toArray(
                    new String[userMacSize]);
            for (String mac : userMacArr) {
                long[] timeArr = dataMap.get(mac);
                int dwell = timeArr.length * 60;
                System.out.println("老Dwell:" + dwell);
                // 获取用户当前停留信息
                DBObject ref = new BasicDBObject();
                ref.put("user_mac", mac);
                ref.put("planar_graph", storeId);
                ref.put("update_time", updateTime);
                BasicDBObject keys = new BasicDBObject();
                keys.append("dwell", 1);
                keys.append("time_stamp_list", 1);
                DBObject dbo = dbCollection.findOne(ref, keys);
                List<Long> oldStampList = new LinkedList<Long>();
                if (dbo != null) {
                    dwell += Integer.valueOf(dbo.get("dwell").toString());
                    if (dbo.get("time_stamp_list") != null) {
                        oldStampList = (List<Long>) dbo.get("time_stamp_list");
                    }
                }
                System.out.println("新Dwell:" + dwell + "\t原时间戳版本数据："
                        + oldStampList.size());
                int l = timeArr.length;
                for (int j = 0; j < l; j++) {
                    oldStampList.add(timeArr[j]);
                }

                List<Long> resStampList = new LinkedList<Long>();
                Set<Long> resStempSet = new HashSet<Long>();
                resStempSet.addAll(oldStampList);
                resStampList.addAll(resStempSet);

                System.out.println("最终Dwell:" + resStampList.size() * 60
                        + "\t最终时间戳版本数据：" + resStampList.size());
                DBObject item = new BasicDBObject();
                item.put("user_mac", mac);
                item.put("dwell", resStampList.size() * 60);
                item.put("create_time", createTime);
                item.put("update_time", updateTime);
                item.put("planar_graph", storeId);
                item.put("time_stamp_list", resStampList);
                dbCollection.update(ref, item, true, false);
            }
        }
        mongo.close();
    }

}