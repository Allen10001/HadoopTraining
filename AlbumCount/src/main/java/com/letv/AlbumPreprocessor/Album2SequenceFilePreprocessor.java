package com.letv.AlbumPreprocessor;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Album2SequenceFilePreprocessor extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(Album2SequenceFilePreprocessor.class);

    public int run(String[] args) throws Exception{

        Job job = Job.getInstance(getConf(),"Transport Album Data to SequenceFile");
        job.setJarByClass(Album2SequenceFilePreprocessor.class);
        job.setMapperClass(CleanerMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{

        int exitCode = ToolRunner.run(new Album2SequenceFilePreprocessor(),args);
        System.exit(exitCode);
        /*
        Configuration configuration = new Configuration();
        try{
            Job job = Job.getInstance(configuration,"Transport Album Data to SequenceFile");
            job.setJarByClass(Album2SequenceFilePreprocessor.class);
            job.setMapperClass(CleanerMapper.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(0);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(job,new Path(args[0]));
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0:1);
        }catch (Exception e){
            logger.error("main error, ",e);
        }*/
    }
}

