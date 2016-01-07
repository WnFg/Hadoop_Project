//记录一个类中的文件个数。


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class my_CountClass {	
   //  输入序列化文件,形式为 <类名，"文件1，文件2.......">
    public static class ClassNum_Map extends Mapper<Text, Text, Text, IntWritable>{
		private final IntWritable one = new IntWritable(1); 
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
				String str = value.toString();  
				StringTokenizer token = new StringTokenizer(str, ",");   // 对"文件1，文件2..."分词  
            	while (token.hasMoreTokens()) {                 
					token.nextToken();
                	context.write(key, one);      //  输出 <类名，[1,1......]>
			}
		}
	}

	public static class ClassNum_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int docSum = 0;  
			for (IntWritable val : values) {  
			    docSum += val.get();  
			}  
			context.write(key, new IntWritable(docSum));  		//  输出 <类名，文件数>
		}
	}
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(my_CountClass.class); 		
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);   
        job.setMapperClass(ClassNum_Map.class);  
        job.setReducerClass(ClassNum_Reduce.class);   
        job.setInputFormatClass(SequenceFileInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);   
        FileInputFormat.addInputPath(job,new Path(args[0]));  
        FileOutputFormat.setOutputPath(job,new Path(args[1]));   
        job.waitForCompletion(true); 
    }
}
