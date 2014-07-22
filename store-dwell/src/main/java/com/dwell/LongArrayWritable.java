package com.dwell;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class LongArrayWritable extends ArrayWritable {

    /**
     * 2014-6-6上午11:14:52
     * 
     * @author fsl shilei.feng@palmaplus.com
     * @param args
     */
    public LongArrayWritable() {
        super(LongWritable.class);
    }

    public LongArrayWritable(LongWritable[] values) {
        super(LongWritable.class, values);
    }

//    public LongWritable[] get() {
//        Writable[] vals = this.get();
//        int length = vals.length;
//        LongWritable res[] = new LongWritable[length];
//        for(int i =0;i<length;i++)
//        {
//            res[i] = new LongWritable(Long.valueOf(vals[i].toString()));
//        }
//        return res;
//    }
}
