import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;


public class my_Hash{	

	public class Pair {
		public double first;
		public double second;
		Pair(double a, double b){
			first = a;
			second = b;
		}
	}
	private static HashMap<String, Pair> class_Num_Prob = new HashMap<String, Pair>();//  先验概率 <类名, <文件数，概率>>
	public static HashMap<String, Pair> Get_class_Num_Prob() throws IOException   		{		
		Configuration conf = new Configuration();
		
		String filePath = "hdfs://127.0.1.1:9000/user/wfang/country_dict/output/part-r-00000";
		
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		
			System.out.println("sdfsdf");
		Path path = new Path(filePath);
		SequenceFile.Reader reader = null;
		double totalDocs = 0;
		
		try {

			reader = new SequenceFile.Reader(fs, path, conf); //创建Reader对象
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();//获取当前读取的字节位置。设置标记点，标记文档起始位置，方便后面再回来遍历
			while (reader.next(key, value)) {
				totalDocs += value.get();//得到训练集文档总数
			}
			reader.seek(position);//重置到前面定位的标记点
			while(reader.next(key, value)){
				class_Num_Prob.put(key.toString(), new my_Hash().new Pair(value.get(), value.get()/totalDocs));//各类文档的概率 = 各类文档数目/总文档数目
				//System.out.println(key+":"+value.get()+"/"+totalDocs+"\t"+value.get()/totalDocs);
			}

		}finally {
			
			IOUtils.closeStream(reader);
		}			
		//验证是否得到先验概率
		for(Map.Entry<String, Pair> entry:class_Num_Prob.entrySet()){
			String mykey = entry.getKey().toString();
			double myvalue = entry.getValue().second;
			 //Double.parseDouble(
			System.out.println(mykey + "\t" + myvalue + "\t");
		}		
		return class_Num_Prob;	
	}
	public static void test() throws IOException{
		Get_class_Num_Prob();									
	}
		public static void main(String[] args){
		System.out.println("sdf");
		try{test();}catch(IOException e){}
	}

}
