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

	public static class Pair {
		public double first;
		public double second;
		Pair(double a, double b){
			first = a;
			second = b;
		}
	}
	/*
		计算hash表，输出的hash表为 hash[“类名”] = <文件数， 概率>， 意义为类A中的文件总数，及其在所有文件中所占比例。
	*/
	private static HashMap<String, Pair> class_Num_Prob = new HashMap<String, Pair>();//  先验概率 <类名, <文件数，概率>>
	public static HashMap<String, Pair> Get_class_Num_Prob() throws IOException   		{		
		Configuration conf = new Configuration();
		
		String filePath = "hdfs://localhost:9000/user/wfang/country_dict/output/part-r-00000";
		
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		
		System.out.println("sdfsdf");
		Path path = new Path(filePath);
		SequenceFile.Reader reader = null;
		double totalDocs = 0;
		
		try {

			reader = new SequenceFile.Reader(fs, path, conf); //创建Reader对象
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long start = reader.getPosition();   // 标记文件的起始位置
			while (reader.next(key, value)) {
				totalDocs += value.get();//得到训练集文档总数
			}
			reader.seek(start);
			while(reader.next(key, value)){
				class_Num_Prob.put(key.toString(), new Pair(value.get(), value.get()/totalDocs));
				//类中文件出现的概率 = 类中文件数 / 总文件数。  记录类中文件数及其概率。
			}

		}finally {
			
			IOUtils.closeStream(reader);
		}			
		//验证是否得到先验概率
/*		for(Map.Entry<String, Pair> entry:class_Num_Prob.entrySet()){
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
	*/
	}
	
	/*  
	   
	   计算hash表，输出的hash表为 hash["类别，单词"] = 概率，意义是 类A下单词W出现的概率 
		
	*/
	private static HashMap<String, Double> wordProb = new HashMap<String, Double>();
	public static HashMap<String, Double> Get_wordProb() throws IOException{
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
				newKey.set(key.toString().substring(0, index));//得到单词所在类的类名,以便后面根据类名查找对应类的文件数目
			
				wordProb.put(key.toString(), (value.get()*1.0)/(class_Num_Prob.get(newKey.toString()).second));
				
			}
		}finally{
			IOUtils.closeStream(reader);
		}
		//验证是否得到条件概率
//		for(Map.Entry<String, Double> entry:wordProb.entrySet()){
//			String mykey = entry.getKey();
//			double myvalue = entry.getValue();
//			System.out.println(mykey + "\t" + myvalue);
//		}		
		return wordProb;		
	}
	
	public static class Doc_Class_Map extends Mapper<Text, Text, Text, Text>{
		public void setup(Context context)throws IOException{
			Get_class_Num_Prob();
			Get_wordProb();
		}
		
		private Text newKey = new Text();
		private Text newValue = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			int index = key.toString().indexOf(",");
			String docID = key.toString().substring(index + 1, key.toString().length());
			
			for(Map.Entry<String, Double> entry:class_Num_Prob.entrySet()){
				//遍历所有类别
				String mykey = entry.getKey();
				newKey.set(docID);//新的键值的key为<文档名>
				double tempValue = Math.log(entry.getValue());//构建临时键值对的value为各概率相乘,转化为各概率取对数再相加				
				StringTokenizer itr = new StringTokenizer(value.toString());				
				while(itr.hasMoreTokens()){
					//遍历一份测试文档中的所有单词				
					String tempkey = mykey + "," + itr.nextToken();//构建字符串"class,word", 在wordProb表中查找对应的概率						
					if(wordProb.containsKey(tempkey)){
						//测试文档的单词在训练集中出现过
						tempValue += Math.log(wordProb.get(tempkey));
					}else{
						//如果测试文档中的单词没有在训练集中出现过则平滑
						tempValue += Math.log(1.0 / (class_Num_Prob.get(mykey).first + 2));
					}
				}
				newValue.set(mykey + "," + tempValue);  //新的value为<类名,概率>,即<class,probably>
				context.write(key, newValue);//一份文档遍历在一个类中遍历完毕,则将结果写入文件,即<docID,<class,probably>>
				//System.out.println(newKey + "\t" + newValue);
			}
		}
	}
	public static class Doc_Class_Reduce extends Reducer<Text, Text, Text, Text>{
		Text newValue = new Text();	
		public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			boolean flag = false;//标记,若第一次循环则先赋值,否则比较若概率更大则更新
			String tempClass = null;
			double tempProbably = 0.0;
	        for (Text value : values) {//对于每份文档找出最大的概率所对应的类 
	        	int index = value.toString().indexOf(",");	        	
        		if(flag != true){
	        		tempClass = value.toString().substring(0, index);
	        		tempProbably = Double.parseDouble(value.toString().substring(index+1, value.toString().length()));	        		
	        		flag = true;
	        	}else{
	        		if(Double.parseDouble(value.toString().substring(index+1, value.toString().length())) > tempProbably)
	        			tempClass = value.toString().substring(0, index);
	        			//tempProbably = Double.parseDouble(value.toString().substring(index+1, value.toString().length()));	
	        	}
        	}	        	
	        
        	newValue.set(tempClass);      	
	        context.write(key, newValue);	
	        System.out.println(key + "\t" + newValue);
	    }
	}	
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();  
		Job job = new Job(conf); 
        job.setJarByClass(Prediction.class);  	
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(DoubleWritable.class);  
        job.setMapperClass(PredictionMap.class);  
        job.setReducerClass(PredictionReduce.class);  
        job.setInputFormatClass(SequenceFileInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
        FileInputFormat.addInputPath(job,new Path(args[0]));  
        FileOutputFormat.setOutputPath(job,new Path(args[1]));     
        job.waitForCompletion(true); 
    }
}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	
	
