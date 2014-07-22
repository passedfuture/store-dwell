package com.dwell;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MapClass extends TableMapper<Text, LongArrayWritable> {
    private MongoClient mongo;
    private DB db;
    private DBCollection dbCollection;
    private DBCollection userSettingsCollection;
    private Map<String, Integer> macStoreMap = new HashMap<String, Integer>(); // 存放mac地址与店铺id
                                                                               // 的对应关系
    private Map<String, String> userSettingsMap = new HashMap<String, String>(); // 存放不同店铺用户的设定起止时间段

    public void setup(Context context) throws UnknownHostException {

        mongo = new MongoClient(Arrays.asList(new ServerAddress("10.1.8.45",
                27017), new ServerAddress("10.1.8.46", 27017),
                new ServerAddress("10.1.8.47", 27017)));

        db = mongo.getDB("bi_single_store");

        dbCollection = db.getCollection("store_apmac_relation");
        userSettingsCollection = db.getCollection("user_settings");
        BasicDBObject ref = new BasicDBObject();
        BasicDBObject keys = new BasicDBObject();
        keys.append("apmac", 1).append("store_id", 1);
        DBCursor dbc = dbCollection.find(ref, keys);
        while (dbc.hasNext()) {
            DBObject dbo = dbc.next();
            macStoreMap.put(dbo.get("apmac").toString().toUpperCase(),
                    Integer.valueOf(dbo.get("store_id").toString()));
        }

        // 获取每个店铺的用户设置信息
        BasicDBObject refSettings = new BasicDBObject();
        BasicDBObject keysSettings = new BasicDBObject();
        keysSettings.append("start_time", 1).append("end_time", 1)
                .append("planar_graph", 1);
        DBCursor userSettingsCursor = userSettingsCollection.find(refSettings,
                keysSettings);

        while (userSettingsCursor.hasNext()) {
            DBObject dbo = userSettingsCursor.next();
            userSettingsMap.put(dbo.get("planar_graph").toString()
                    + "_startTime", dbo.get("start_time").toString());
            userSettingsMap.put(
                    dbo.get("planar_graph").toString() + "_endTime",
                    dbo.get("end_time").toString());
        }

        Configuration conf = context.getConfiguration();
    }

    public void map(ImmutableBytesWritable key, Result values, Context context)
            throws IOException, InterruptedException {

        SimpleDateFormat sdfTime = new SimpleDateFormat("yyyy-MM-dd   HH:mm:ss");
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");

        // 解析key
        byte[] macArr = key.get();
        StringBuilder apMac = new StringBuilder("");
        StringBuilder userMac = new StringBuilder("");

        for (int i = 0; i < macArr.length; ++i) {
            byte b = macArr[i];
            if (i < 6) {
                apMac.append(String.format("%02x", b));
                if (i != 5)
                    apMac.append(':');
            } else {
                userMac.append(String.format("%02x", b));
                if (i != macArr.length - 1)
                    userMac.append(':');
            }
        }
        String storeId = "0";
        if (macStoreMap.containsKey(apMac.toString().toUpperCase())) {
            storeId = macStoreMap.get(apMac.toString().toUpperCase()) + "";
        }
        // 获取对应店铺的时间段设置，若没有设置则开始时间默认为00:00:00 结束时间设置为24:00:00
        String startTimeStr = "00:00:00";
        String endTimeStr = "23:59:59";
        if (userSettingsMap.containsKey(storeId + "_startTime")) {
            startTimeStr = userSettingsMap.get(storeId + "_startTime");
        }
        if (userSettingsMap.containsKey(storeId + "_endTime")) {
            endTimeStr = userSettingsMap.get(storeId + "_endTime");
        }

        Cell cellArr[] = values.rawCells();
        Set<LongWritable> timeSet = new HashSet<LongWritable>();

        // 计算设定时间范围
        long startTime = 0l;
        long endTime = 0l;
        try {
            startTime = sdfTime
                    .parse(sdfDate.format(new Date()) + startTimeStr).getTime();
            endTime = sdfTime.parse(sdfDate.format(new Date()) + endTimeStr)
                    .getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        for (Cell c : cellArr) {
            long timeStamp = c.getTimestamp();
            //
            if (timeStamp >= startTime && timeStamp <= endTime) {
                timeSet.add(new LongWritable(timeStamp / (60 * 1000)
                        * (60 * 1000)));
            }
        }
        LongWritable timeStampArr[] = timeSet.toArray(new LongWritable[timeSet
                .size()]);
        // 此处可以去除信号较弱的点
        // int time = cellArr.length * timeRange;
        context.write(new Text(storeId + "_" + userMac), new LongArrayWritable(
                timeStampArr));
    }

    public void cleanup(Context context) throws UnknownHostException {
        mongo.close();
    }
}
