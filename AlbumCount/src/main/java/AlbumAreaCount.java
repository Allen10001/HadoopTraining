import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * describe:
 * 查找不同地区的专辑总数，并按照value(专辑总数)降序
 */
public class AlbumAreaCount extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(AlbumAreaCount.class);

    private static class AlbumCountByAreaMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text EXCEPTION = new Text("异常数据");

        private static int unnormalCount = 0;

        public void map(Object key, Text value, Context context) {
            String[] albumDetails = value.toString().split("\\t");
            logger.info("album fields' length, {}", albumDetails.length);
            // Pattern pattern = Pattern.compile("^(香港|中国大陆|台湾|澳门);$");
            try {
                if (albumDetails.length == 23) {
                    context.write(new Text(albumDetails[15]), one);
                } else {
                    context.write(EXCEPTION, one);
                    unnormalCount++;
                    logger.error("unnormal data,len={}, id={}", albumDetails.length, albumDetails[0]);
                }
            } catch (Exception e) {
                logger.error("failed to count,id {}", albumDetails[0]);
            }
        }

        public void cleanup(Context context) {
            try {
                super.cleanup(context);
                logger.error("unnormalCount:{}", unnormalCount);
            } catch (Exception e) {
                logger.error("map cleanup error, {}", e);
            }
        }
    }

    private static class AlbumCountByAreaReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable res = new IntWritable();
        private ArrayList<AlbumBean> list = new ArrayList<>(32);

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable item : values) {
                sum += item.get();
            }
            res.set(sum);
            try {
                list.add(new AlbumBean(new Text(key.toString()), new IntWritable(sum)));
                // context.write(key, res);
            } catch (Exception e1) {
                logger.error("error calculate sum, id: {}, {}", key, e1);
            }
        }

        /**
         * 对group by 的结果进行排序的话需要经过两次 mr。
         * 第一个MR执行GROUP & SUM操作
         * 第二次会对sum(Wcount) as wc 进行二次排序
         * 所以，如果想利用MapReduce对Reduce结果进行二次排序，则最少需要两次MapReduce(第二次可以只有Map,不用Reduce,
         * 只需要把第一次MR的输出结果作为第二次MR的输入即可。
         *
         * 如果Reduce输出量很小（并且，Reduce的数据量 = 1时）的情况下，可以在cleanup() 中对结果进行排序。
         * 下面是在cleanup() 中进行排序的方法。
         * @param context
         */
        public void cleanup(Context context) {

            logger.info("list length: {}",list.size());
            list.sort(new Comparator<AlbumBean>() {
                public int compare(AlbumBean o1, AlbumBean o2) {
                    if (o1.getCount().get() == o2.getCount().get()) {
                        return 0;
                    } else {
                        return -(o1.getCount().get() - o2.getCount().get());
                    }
                }
            });
            try {
                for (AlbumBean item : list) {
                    context.write(item.getArea(), item.getCount());
                }
            }catch (Exception e){
                logger.error("{}",e);
            }
        }
    }

    public int run(String[] args){
        Configuration configuration = new Configuration();
        Job job = null;
        int res = 1;
        try {
            job = Job.getInstance(configuration, "album count");
            job.setJarByClass(AlbumAreaCount.class);
            job.setMapperClass(AlbumCountByAreaMapper.class);
            job.setCombinerClass(AlbumCountByAreaReducer.class);
            job.setReducerClass(AlbumCountByAreaReducer.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));   // InputFormat 负责产生分片，并将他们分割成记录
            FileOutputFormat.setOutputPath(job, new Path(args[1]));  // todo  ??
            res = job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            logger.error("Execute error, {}", e);
        }
        return res;
    }
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AlbumAreaCount(),args));
    }
}


