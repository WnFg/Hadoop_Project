import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

public class my_clacPRF{	
	public static Vector<String> v = new Vector<String>();
	public static HashSet<String> hset = new HashSet<String>();
	
	public static void evaluate(String filepath){
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		SequenceFile.Reader reader = null;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			while(reader.next(key, value)){				// 把分类后的数据读入到v中。v[i]为真实类别，v[i+1]为预测类别，i为偶数
				int index = key.toString().indexOf(",");

				String trueClass = key.toString().substring(0, index);
				v.add(trueClass);
				v.add(value.toString());
				hset.add(trueClass);				// 把所有的类存入HashSet.
			}
		}
		IOUtils.closeStream(reader);
		
		// 计算Precision，Recall, F1
		int ClassSum = hset.size();
		int f = 0;
		double TP=0.0,FN=0.0,FP=0.0,TN=0.0;
		double P,R,F;
		double Precision[]=new double[ClassSum];
		double Recall[]=new double[ClassSum];
		double F1[]=new double[ClassSum];
		double sumuP=0.0,sumR=0.0,sumF=0.0;
		for(String c : hset){
            for(int i = 0; i < v.size(); i += 2){
             if(c.equals(v[i])&&c.equals(v[i+1])) TP++;   
             else if(c.equals(v[i])&&!c.equals(v[i+1])) FN++;
             else if(!c.equals(v[i])&&c.equals(v[i+1])) FP++;
             else if(!c.equals(v[i])&&!c.equals(v[i+1]))  TN++;
            }
            P=TP/(TP+FP);  //Precision
            R=TP/(TP+FN);   //Recall
            F=2*P*R/(P+R);   //F1
            Precision[f]=P;
    		Recall[f]=R;
    		F1[f]=F; 
			f++;
    		TP=0.0;FN=0.0;FP=0.0;TN=0.0;
		}
		for(int i=0;i<ClassSum;i++){
    		sumuP+=Precision[i];
    		sumR+=Recall[i];
    		sumF+=F1[i];
        }
		System.out.println("平均Precision精度值："+sumuP/ClassSum);
        System.out.println("平均Recall精度值："+sumR/ClassSum);
        System.out.println("平均的调和均值："+sumF/ClassSum);
	}
	
	public static void main(String[] args) throws Exception {
		evaluate(args[0]);
	}
}
