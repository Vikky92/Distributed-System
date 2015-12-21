/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CorpusCaculator{
	
	private static final String OUTPUT_PATH = "intermediate_output";
	private static final String OUTPUT_PATH1 = "intermediate_output1";
	private static final String OUTPUT_PATH2 = "intermediate_output2";

	//To round of the decimal places in the probability to resolve NumberFormat errors 
	static String roundOffTo2DecPlaces(double val)
	{
	    return String.format("%.99f", val);
	}
	
	static String roundOffSentence(double val)
	{
	    return String.format("%.99f", val);
	}

/*The first map-reduce divides the corpus into sentences and finds the count of the tokenized words in the sentences
	Also, the sentences associated with a word are carried along with the value. This is done to ensure that sentences can be assigned 
	probabilities later in the process
	*/
  public static class Mapper1 extends Mapper<Object, Text, Text, Text>{
    
    private Text val = new Text(); 
    private Text word = new Text();
	static TreeSet<String> str = new TreeSet<String>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	   //This is used to divide sentences in the corpus using a regex expression 
    	   String line = value.toString();
	       Pattern re = Pattern.compile("[^.!?\\s][^.!?]*(?:[.!?](?!['\"]?\\s|$)[^.!?]*)*[.!?]?['\"]?(?=\\s|$)", Pattern.MULTILINE | Pattern.COMMENTS);
	       Matcher reMatcher = re.matcher(line);
	       ArrayList<String> ar = new ArrayList<String>() ; 
	       //The sentences are then added to an ArrayList 
	       while (reMatcher.find()) 
	           ar.add(reMatcher.group()) ; 	           
	       //Tokenizing the sentences to find the positions and their counts 
	       int i = 0 ; 
	       for(String temp: ar) { 
	    	   temp = temp.replace(",","");
	    	   StringTokenizer tokenizer = new StringTokenizer(temp) ; 

	    	   while(tokenizer.hasMoreTokens()) {

	    		  i++ ; 
	    		  String a =  i+"\t"+ tokenizer.nextToken(); 
	    		  word.set(a) ;
	    		  str.add(temp);
	    		  val.set("1"+"\t"+str) ; 
	    		  
	    		  context.write(word,val) ; 
	    	   }
	    	   str.clear();
	       }
	   }
  }
  
  public static class Reducer1  extends Reducer<Text,Text,Text,Text> {
	  Text a = new Text();
  	String ab = new String();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	int sum = 0;
    	TreeSet<String> tree1 = new TreeSet<String>();
       for (Text val : values) {
	        String temp = val.toString();
	        String[] str = temp.split("\t") ; 
	        sum+=Integer.parseInt(str[0]) ; //finding the sum of occurances of the word 
	        tree1.add(str[1]) ; 
	        a.set(sum + "\t"+ tree1) ; 
       }
       
 	      context.write(key,a); //Position Word Count 
 	      }
 	   }
 //This map-reduce phase is used to find the sum and in turn the probability of the words in the corpus 
  public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
	    
	    private Text word = new Text();
	     private Text temp  =  new Text() ;

	      
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String line = value.toString();
		       String[] a = line.split("\t") ;
		       word.set(a[1]+"\t"+a[2]+"\t"+a[3]) ; //forming the <key,value> pairs 
		       temp.set(a[0]) ;
		       context.write(temp,word); //Position	  Word	 Count

	  }
  }
  
  public static class Reducer2  extends Reducer<Text,Text,Text,Text> {
	     private Text val = new Text();  //For output

	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	    	double sum=0;

			   ArrayList<String> cache = new ArrayList<String>()  ;
			   String temp = new String();		   

			   for(Text val:values) { 
				   temp = val.toString() ;  
				   
				   cache.add(temp) ;  //Adding to cache as the Iterable cannot be traversed twice 
				   
				   String a[] = temp.split("\t");  
				   sum+=Double.parseDouble(a[1]) ;  //finding the sum of the words in the corpus 
			   }
			 //This for loop is used to find the probability of words in the corpus 
			   for(String s : cache) {  
				   String[] b = s.split("\t") ;   
				   String prob = roundOffTo2DecPlaces(Double.parseDouble(b[1])/sum) ; 
				   String b1 = b[0]+"\t"+prob+"\t"+b[2] ; 
				   val.set(b1) ; // Word	Prob	List of sentences 
				   context.write(key, val);

			   }
	 	     }
	 	   }
  /*
   * This map-reduce phase is used to calculate the probability of the sentences and produce the <val,sentence> pairs 
   * This is done as the input to Mapper3 is <Position,Word Count> from the previous stage 
   */
  public static class Mapper3 extends Mapper<Object, Text, Text, Text>{
	    
	    private Text val = new Text(); 
	    private Text sentence = new Text();
	      
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	    	/* Upon extracting the list of sentences with the word occcurances we need to extract them to find the existence probability 
	    		Parsing the sentence is done for this purpose 
	    		*/
	    	String[] str=line.split("\t");
	    	str[3] = str[3].replace("[","");
	    	str[3] = str[3].replace("]",""); 
	    	
	    	
	    	String[] ab = str[3].split(", ") ; 
	    	for(int i =0; i< ab.length ; i++) {
	    		sentence.set(ab[i]);
		    	val.set(str[0]+"\t"+str[1]+"\t"+str[2]) ; 
		    	context.write(sentence,val); 		
	    	}
		   }
	  }
  public static class Reducer3  extends Reducer<Text,Text,Text,Text> {
	   Text val = new Text(); //For sentence probability
	   Text sentence = new Text() ; //For the sentence 
	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

	    	   double SentenceProb=1.0;
			   String temp = new String();	
			   
	    	   for(Text val1 : values) {
	    			temp = val1.toString();
	    			String[] str = temp.split("\t") ;
	    			SentenceProb= Double.parseDouble(roundOffSentence(Double.parseDouble(str[2])*SentenceProb));	    			
	    		}
	    		val.set(SentenceProb+"");
	    		sentence.set(key);
	    		
	    		context.write(val,sentence);
	 	     }
	 	   }
  
  /*
   * This map-reduce phase is used to sort and display the top3 sentences with their respective probabilities
   * a treemap has been used for this purpose as it naturally sorts the keys in the ascending order
   * reverseOrder() has been used to sort the values in the ascending order for the TOP3 as needed 
   * 
   * A NullWritable has also been used to ensure that all the output are grouped into one reducer based 
   * MapReduce framework provides us with a cleanup function that conveniently runs after the last map function runs.
   * This is used to write the output using context.write
   */
  
  public static class Mapper4 extends Mapper<Object, Text, NullWritable, Text>{
	    
	     private TreeMap<Double,Text> TOP3 = new TreeMap<Double,Text>() ; 
	     Text val = new Text(); 
	     Text sentence = new Text();
	      
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String v[] = value.toString().split("\t");
	    	Double prob = Double.parseDouble(v[0]) ; 
	    	TOP3.put(prob,new Text(prob+"\t"+v[1])) ; 
	    	
	    	if(TOP3.size() >3) 
	    		TOP3.remove(TOP3.firstKey()) ; 

	    	for(Text txt : TOP3.values()) 
				System.out.println(txt);
			
		   }
	    
	    protected void cleanup(Context context) throws IOException,InterruptedException {
			
			for(Text txt : TOP3.values()) {
				System.out.println(txt);
				context.write(NullWritable.get(),txt) ; 
			}
		}
	  }
  
  public static class Reducer4  extends Reducer<NullWritable,Text,NullWritable,Text> {
	  
	  private  TreeMap<Double,Text> TOP3 =new TreeMap <Double,Text>(Collections.reverseOrder()) ; 
	    public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

	    	   
	    	   for(Text val : values) {
	    		   String v[] = val.toString().split("\t") ; 
	    		   
	    		   TOP3.put(Double.parseDouble(v[0]),new Text(v[0]+"\t"+v[1])); 
	    		   
	    		   if(TOP3.size() >3 ) 
	   	    		   TOP3.remove(TOP3.firstKey());
	    	   }
	    	   
	    	   for(Text t : TOP3.values()) {
	    		   context.write(NullWritable.get(),t); 
	    		   
	    	   }
	 	     }
	 	   }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //This is used to delete the existing directories for the outputs and intermediates if needed 
    FileSystem fs = FileSystem.get(conf) ; 
    if(fs.exists(new Path(args[1]))) {
    	fs.delete(new Path(args[1]),true) ; 
    	
    }
    if(fs.exists(new Path(OUTPUT_PATH))) {
    	fs.delete(new Path(OUTPUT_PATH),true) ; 
    	
    }
    
    if(fs.exists(new Path(OUTPUT_PATH1))) {
    	fs.delete(new Path(OUTPUT_PATH1),true) ; 
    	
    }
    
    if(fs.exists(new Path(OUTPUT_PATH2))) {
    	fs.delete(new Path(OUTPUT_PATH2),true) ; 
    	
    }
    
    //
    Job job = new Job(conf, "Tokenization");
    job.setJarByClass(CorpusCaculator.class);
    job.setMapperClass(Mapper1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
    
    job.waitForCompletion(true) ;
    
    Configuration conf1 = new Configuration();
    
    Job job1 = new Job(conf1, "Summation");
    job1.setJarByClass(CorpusCaculator.class);
    job1.setMapperClass(Mapper2.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setCombinerClass(Reducer2.class);
    job1.setReducerClass(Reducer2.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(OUTPUT_PATH));
    FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH1));
    
    job1.waitForCompletion(true);
    
    
    Configuration conf2 = new Configuration();
    
    Job job2 = new Job(conf2, "Probability");
    job2.setJarByClass(CorpusCaculator.class);
    job2.setMapperClass(Mapper3.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setReducerClass(Reducer3.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH1));
    FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH2));
    
    job2.waitForCompletion(true);
    
 Configuration conf3 = new Configuration();
    
    Job job3 = new Job(conf3, "Sort and TopN");
    job3.setJarByClass(CorpusCaculator.class);
    job3.setNumReduceTasks(1);
    job3.setMapperClass(Mapper4.class);
    job3.setReducerClass(Reducer4.class);
    job3.setMapOutputKeyClass(NullWritable.class);
    job3.setMapOutputValueClass(Text.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(OUTPUT_PATH2));
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
   
    job3.waitForCompletion(true);
    
    return;
  }
}
  

