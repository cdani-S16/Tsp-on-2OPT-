import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TSPHadoop {

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, Text>{
    
    //calculate eucledian distance
    static double eucdistance (float x1, float y1, float x2, float y2) {
	    float xval = Math.abs(x2 - x1);
	    float yval = Math.abs(y2 - y1);
	    double distance = Math.sqrt((yval)*(yval) +(xval)*(xval));
	    
	    return distance; 
	    }


	static double cost(String path, double[][] distancematr)
	{
		double cost = 0;
		String[] strAr = path.split(" ");
		int noOfCities = strAr.length;
		System.out.println("Path : " + path + " \nstrar length " + strAr.length);
		
		for(int k = 1; k< noOfCities-1; k++)
		{
			//System.out.println("first "+(strAr[k]));
			//System.out.println("second " +(strAr[k+1]));
			cost = cost + distancematr[Integer.parseInt(strAr[k])][Integer.parseInt(strAr[k+1])];
		}
		
		return cost;
	}

    	static double calcDist(int i , int j, String path, double[][] distancematr)
	{
		//if(i == 1 && j == 5)
		//	System.out.println(" i is 1 da and j is 5");
		String[] strAr = path.split(" ");
		int noOfCities = strAr.length;
		double dist = distancematr[Integer.parseInt(strAr[i+1])][Integer.parseInt(strAr[j+1])];
		//System.out.println("cal dist from :" + Integer.parseInt(strAr[i+1]) + 
		//		" to " + Integer.parseInt(strAr[j+1]) +
		//		" is " + dist);
		//System.out.println("cal dist");
		
		
		return dist;
	}

    static String swap(String route, int i, int j) throws InterruptedException
	{
		String rte = "";
		String[] strList = route.split(" ");
		String[] newlist = new String[strList.length];
		int k;
		for(k = 0; k< i+2; k++){
			newlist[k] = strList[k];
		}
		for(int p = j+1; k< j+2; k++, p-- ){
			newlist[k] = strList[p];
		}
		for(;k< strList.length; k++){
			newlist[k] = strList[k];
		}
		for(int p = 1; p < strList.length; p++){
			rte  = rte + " " + newlist[p];
		}
		return rte;
	}

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      /*StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);*/
        String[] arData = value.toString().split(" ");
		Float[][] points = new Float[(arData.length/3)+1][2];
		int dataSize = (arData.length/3);
		//word.set(Integer.toString(dataSize));
        //context.write(word,one);
			
		IntWritable curkey = new IntWritable(1);//Integer.parseInt(arData[0]));
		
		LinkedList<Integer> arcnt = new LinkedList<Integer>();
		int j = 0;
		for(j = 1; j< dataSize+1; j++)
		{
			//String[] vals = data.get(j-1).split(" ");
			points[j][0] = Float.parseFloat(arData[((j-1)*3)+1]);
			points[j][1] = Float.parseFloat(arData[((j-1)*3)+2]);
			arcnt.add(Integer.parseInt(arData[((j-1)*3)+0]));
			//word.set(Integer.toString(j));
        	//context.write(word,one);
		}
		
		System.out.println("No of Cities after reading from the file :" + arcnt.size());

        double[][] distancematr = new double[dataSize+1][dataSize+1];
		for(int i = 1; i< dataSize; i++)
			for(j = 1; j< dataSize; j++)
				distancematr[i][j] = eucdistance(points[i][0], points[i][1], points[j][0], points[j][1]);
		
        Integer[] ar = {0};
		int noOfCities = dataSize;
		//System.out.println("No of Cities :" + noOfCities);

		String initRoute = "";
		double totCost = 0;
		//making the initial route
		int k =0;
		for(k = 0; k< noOfCities; k++)
			{
				initRoute = initRoute + " " + (k+1);
				totCost = totCost + distancematr[k][k+1];
			}
		
		//adding edge back to start point
		//totCost = totCost + distancematr[k][1];
		//initRoute = initRoute + " " + "1";
		
        String Route = initRoute;

        double minchange = 0, change = 0;
		int mini = 0, mink = 0;
		int ctr = 0;

        do {
			mini = 0;
			mink = 0;
			minchange = 0;
			for(int i = 0; i < noOfCities-2; i++)
			{
				for(k = i+2; k< noOfCities ; k++)
				{

					change = calcDist(i, k, Route, distancematr) 
							+ calcDist(i+1, k+1, Route, distancematr)
							- calcDist(i, i+1, Route, distancematr)
							- calcDist(k, k+1, Route, distancematr);
					
					if(minchange > change){
						minchange = change;
						mini = i; mink = k;
					} 
				}
			}
			ctr++;
			
			//break;
			//swap mini with mink+1 and mini+1 with mink
			Route = swap(Route, mini, mink);
			System.out.println("Current Total cost : " + cost(Route, distancematr));
		} while(minchange < 0);
		
		//reconstruct path file the same as inp format
		String foutput = "";
		String[] newrtearr = Route.split(" ");
		for(int i = 0; i < arcnt.size(); i++)
			System.out.println(arcnt.get(i));
		System.out.println("ggggggggggg");
		for(int i = 1; i < newrtearr.length-1; i++)
		{	
			System.out.println(arcnt.get(Integer.parseInt(newrtearr[i])-1));
			System.out.println(points[i][0]);
			System.out.println("i val " + i + " city: " + arcnt.get(Integer.parseInt(newrtearr[i])-1));
			foutput = foutput + " " + arcnt.get(Integer.parseInt(newrtearr[i])-1) + " " + points[Integer.parseInt(newrtearr[i])][0] + " " + points[Integer.parseInt(newrtearr[i])][1]; 
		}
		System.out.println("new op " + foutput);
		//word.set(Integer.toString(dataSize));
		//context.write(curkey, word);
		//word.set(value.toString());
		//context.write(curkey, word);

		//word.set(Route);
		word.set(foutput);
        context.write(curkey, word);
		//word.set(Double.toString(cost(Route, distancematr)));
		//context.write(curkey, word);
      
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable, Text ,IntWritable,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
	//context.write(new Text("in reducer"),new IntWritable(999));
      int sum = 0;
      for (IntWritable val : values) {
        //sum += val.get();
	//context.write(val,);

      }
      result.set(sum);
      //context.write(result, key);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TSP Hadoop");
    job.setJarByClass(TSPHadoop.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);
    
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
