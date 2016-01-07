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


public class my_predict{	

	public static class Pair {      // 创建类Pair,Pair是一个元组，(double first, double second)。
		public double first;
		public double second;
		Pair(double a, double b){
			first = a;
			second = b;
		}
	}
	/*
	
		读取my_CountClass.java的输出文件，计算先验概率。
		计算hash表，输出的hash表为 hash[“类名”] = <文件数， 概率>，表示的含义为类中的文件总数，及其在所有文件中所占比例。
	
	*/
	private static HashMap<String, Pair> class_Num_Prob = new HashMap<String, Pair>();//  先验概率 <类名, <文件数，概率>>
	public static void Get_class_Num_Prob() throws IOException   		{		
		Configuration conf = new Configuration();
		
		String filePath = "hdfs://localhost:9000/user/wfang/country_dict/output/part-r-00000";  // 文件路径
		
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		
		//System.out.println("sdfsdf");
		Path path = new Path(filePath);
		SequenceFile.Reader reader = null;
		double totalDocs = 0;
		
		try {

			reader = new SequenceFile.Reader(fs, path, conf); //创建Reader对象
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long start = reader.getPosition();   // 标记文件的起始位置
			while (reader.next(key, value)) {
				totalDocs += value.get();		//得到训练集文档总数
			}
			reader.seek(start);
			while(reader.next(key, value)){
				class_Num_Prob.put(key.toString(), new Pair(value.get(), value.get()/totalDocs));
				//类中文件出现的概率 = 类中文件数 / 总文件数。  记录类中文件数及其概率。
			}

		}finally {
			
			IOUtils.closeStream(reader);
		}			

	}
	
	/*  
	   
	   读取my_CountWord.java的输出文件，计算条件概率。
	   计算hash表，输出的hash表为 hash["类别，单词"] = 概率，表达的含义是 类A下单词W出现的概率 
		
	*/
	private static HashMap<String, Double> wordProb = new HashMap<String, Double>();
	public static void Get_wordProb() throws IOException{
		HashMap<String, Double> ClassTotalDocNums = new HashMap<String, Double>();
		Configuration conf = new Configuration();
		
		String filePath = "hdfs://localhost:9000/user/wfang/country/output/part-r-00000";

		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		SequenceFile.Reader reader = null;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			Text newKey = new Text();
			
			while(reader.next(key, value)){				
				int index = key.toString().indexOf(",");
				newKey.set(key.toString().substring(0, index));   //得到单词所在类的类名, 接下来需要根据类名查找类中的文件数
				wordProb.put(key.toString(), (value.get()*1.0)/(class_Num_Prob.get(newKey.toString()).first));
			}
		}finally{
			IOUtils.closeStream(reader);
		}	
	}
	
	public static class classifyMap extends Mapper<Text, Text, Text, Text>{
		
		public void setup(Context context)throws IOException{
			Get_class_Num_Prob();
			Get_wordProb();
		}
		
		private Text newKey = new Text();
		private Text newValue = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			
			//遍历所有类别
			for(Map.Entry<String, Pair> entry:class_Num_Prob.entrySet()){	
				String testClass = entry.getKey();    // 测试类名
				
				double temp_logProb = Math.log(entry.getValue().second);  // 概率相乘取对数后可以转换为相加，防止连乘后出现过小的数。				
				StringTokenizer itr = new StringTokenizer(value.toString(), ",");				
				//遍历测试文件中所有的单词	
				while(itr.hasMoreTokens()){
					String tempkey = testClass + "," + itr.nextToken();	   //构建字符串"class,word", 在wordProb表中查找对应的概率						
					
					if(wordProb.containsKey(tempkey)){
						//测试文档的单词在训练集中出现过
						temp_logProb += Math.log(wordProb.get(tempkey));
					}else{
						//如果测试文档中的单词没有在训练集中出现过则平滑
						temp_logProb += Math.log(1.0 / (class_Num_Prob.get(testClass).first + 2));  // 1 /（文件数 +２）
					}
				}
				newValue.set(testClass + "," + temp_logProb);  
				context.write(key, newValue);		//输出的键值对 key为"真实类名，测试文件名"，value为"测试类名，概率"
			}
		}
	}
	public static class classifyReduce extends Reducer<Text, Text, Text, Text>{
		Text newValue = new Text();	
		public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			int i = 0;   // 记录第几次循环
			String maxProb_Class = null;				//	最大概率的测试类
			double tempProb = 0;
	        for (Text value : values) {//对于每份文档找出最大的概率所对应的类 
	        	int index = value.toString().indexOf(",");	        	
        		if(i == 0){   
					// 第一次循环时直接赋值
	        		maxProb_Class = value.toString().substring(0, index);
	        		tempProb = Double.parseDouble(value.toString().substring(index+1, value.toString().length()));	        		
	        		i++;
	        	}else{ 		
					// 不是第一次循环,要比较大小
					double t = Double.parseDouble(value.toString().substring(index+1, value.toString().length()));
	        		if(t > tempProb){
	        			maxProb_Class = value.toString().substring(0, index);
						tempProb = t;
					}						
	        	}
        	}	        	
	        
        	newValue.set(maxProb_Class);      	
	        context.write(key, newValue);		// 输出的键值对，key是"真实类名，文件名"，value是"概率最大的测试类"	
	    }
	}	
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();  
		Job job = new Job(conf); 
        job.setJarByClass(my_predict.class);  	
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
        job.setMapperClass(classifyMap.class);  
        job.setReducerClass(classifyReduce.class);  
        job.setInputFormatClass(SequenceFileInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
        FileInputFormat.addInputPath(job,new Path(args[0]));  
        FileOutputFormat.setOutputPath(job,new Path(args[1]));     
        job.waitForCompletion(true); 
    }
}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	
	
