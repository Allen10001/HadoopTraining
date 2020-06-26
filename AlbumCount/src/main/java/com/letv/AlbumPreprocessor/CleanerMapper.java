package com.letv.AlbumPreprocessor;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanerMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

        protected void map(IntWritable key, Text value, Context context){
            try{
                context.write(key,value);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

}
