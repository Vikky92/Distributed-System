import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Multimap;

public class Program extends Configured implements Tool{
	
	//Used to check for the counter value in the context of the Reducer 
	public static enum MyCounters {
		Counter;
	}

	/*
	 * Used to insert the values into the Energy Table 
	 * 
	 */
	static class WriteMapper extends Mapper<LongWritable, Text, IntWritable, Text>  {

		IntWritable k = new IntWritable();
		Text res = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String val = value.toString();
			int row = val.hashCode();
			k.set(row);
			res.set(val);
			context.write(k, res);
	    }
	}
	
	public static class WriteReducer extends TableReducer<IntWritable, Text, Text>  {
		  public static final byte[] area = "Area".getBytes(); 
		  public static final byte[] prop = "Property".getBytes();
		  private Text rowkey = new Text();
		  private int rowCount = 0;
		  
		  @SuppressWarnings("deprecation")
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String X1 = "",X2="",X3="",X4="",X5="",
					X6="",X7="",X8="",Y1="",Y2="";
		    for (Text val : values) {
		      String[] v = val.toString().split("\t");
		      X1 = v[0];
		      X2 = v[1];
		      X3 = v[2];
		      X4 = v[3];
		      X5 = v[4];
		      X6 = v[5];
		      X7 = v[6];
		      X8 = v[7];
		      Y1 = v[8];
		      Y2 = v[9];
		    }
		    String k = "row"+rowCount;
		    Put put = new Put(Bytes.toBytes(k.toString()));
		    put.add(area, "X1".getBytes(), Bytes.toBytes(X1));
		    put.add(area, "X5".getBytes(), Bytes.toBytes(X5));
		    put.add(area, "X6".getBytes(), Bytes.toBytes(X6));
		    put.add(area, "Y1".getBytes(), Bytes.toBytes(Y1));
		    put.add(area, "Y2".getBytes(), Bytes.toBytes(Y2));
		    put.add(prop, "X2".getBytes(), Bytes.toBytes(X2));
		    put.add(prop, "X3".getBytes(), Bytes.toBytes(X3));
		    put.add(prop, "X4".getBytes(), Bytes.toBytes(X4));
		    put.add(prop, "X7".getBytes(), Bytes.toBytes(X7));
		    put.add(prop, "X8".getBytes(), Bytes.toBytes(X8));
		    rowCount++;
		    rowkey.set(k);	
		    context.write(rowkey, put);
		  }
	}

	 public static class KMeansMapper extends TableMapper<Text, Result> {
			private Map<String, KeyValue[]> centers;
			
			@Override
			//setup used to load the values of the center table into centers 
			//This function is only called once
			public void setup(Context context) throws IOException, InterruptedException{
				centers = HBase.LoadValue("Center");
			    super.setup(context);
			}
			
			@SuppressWarnings("deprecation")
			public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
				String NearestPoint = HBase.NearestCenter(centers, value.raw());
				context.write(new Text(NearestPoint), value);
			}
		}
		
		public static class KMeansReducer extends TableReducer<Text, Result, ImmutableBytesWritable> {
			private Map<String, KeyValue[]> centers;
			
			@Override
			//setup used to load the values of the center table into centers 
			//This function is only called once
			
			public void setup(Context context) throws IOException, InterruptedException{
				centers = HBase.LoadValue("Center");
				super.setup(context);			
			}
			
			public void reduce(Text key, Iterable<Result> results, Context context)
					throws IOException, InterruptedException {
			
				System.out.println(key);
				List<Double> New_Center = HBase.NewMean(results);
				KeyValue[] Old_Center = centers.get(key.toString());
			
				Double distance = HBase.EucDistance(New_Center, Old_Center);
				
				System.out.println("Distance  "+distance);
				
				//Insert into the table and update the counter only if the difference is more than 0.01
				if (distance > 0.01) {
					
					//Update the Counter
					context.getCounter(MyCounters.Counter).increment(1);
					
					//Write to center table
					Put put = new Put(Bytes.toBytes(key.toString()));
					HBase.InsertRow(put, New_Center);
					context.write(null, put);
				}
			}
		}
	 
	 public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new Program(), args);
	        System.exit(res);
	  }
	 
	@SuppressWarnings({ "resource", "deprecation" })
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		int k = Integer.parseInt(args[1]);
		Configuration conf = HBaseConfiguration.create(); 
		HBaseAdmin admin = new HBaseAdmin(conf) ; 
		
		if(admin.tableExists("Energy"))
		{
			admin.disableTable("Energy") ; 
			admin.deleteTable("Energy") ; 
			System.out.println("******Table Energy already exists..disabling and deleting it*******");
		}
		
		if(admin.tableExists("Center"))
		{
			admin.disableTable("Center");
			admin.deleteTable("Center");
			System.out.println("******Table Center already exists..deleting it*****");			
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("Energy")) ; 
		
		tableDescriptor.addFamily(new HColumnDescriptor("Area")) ; 
		tableDescriptor.addFamily(new HColumnDescriptor("Property")) ; 
	
		admin.createTable(tableDescriptor) ; 
		System.out.println("*****Energy table has been created******");
		
		int iteration = 1; 
		
		String inputPath = args[0];  
		Job LoadJob = new Job(conf,"HBase_Write");        
		LoadJob.setInputFormatClass(TextInputFormat.class);  
		LoadJob.setJarByClass(Program.class);  
		LoadJob.setMapperClass(Program.WriteMapper.class);
		LoadJob.setMapOutputKeyClass(IntWritable.class);
		LoadJob.setMapOutputValueClass(Text.class);
    
		TableMapReduceUtil.initTableReducerJob(
				"Energy",        // output table
				WriteReducer.class,    // reducer class
				LoadJob); //Loading job with MR used to insert values from the dataset.txt file 
		
		LoadJob.setNumReduceTasks(1);   //This job would only require 1 reducer to insert values into the table      
     
		FileInputFormat.setInputPaths(LoadJob, inputPath);  
     
		System.out.println("*****Data imported into Energy table*******");
     
		LoadJob.waitForCompletion(true) ; 
		
		/*
		 * End of the loading job for the input table. 
		 */
     
		//Loading the initial K points into the Center table 
		
		HTableDescriptor tableDescriptor1 = new HTableDescriptor(TableName.valueOf("Center")) ; 
		
		tableDescriptor1.addFamily(new HColumnDescriptor("Area")) ; 
		tableDescriptor1.addFamily(new HColumnDescriptor("Property")) ; 
	
		admin.createTable(tableDescriptor1) ; 
		
		HTable center = new HTable(conf, "Center");
		
		Configuration initialK = HBaseConfiguration.create();
		HTable table = new HTable(initialK,"Energy");
	
		/*
		 * InitialK points for the Center table are taken from the Energy table
		 */
		
		for(int i=0;i<k;i++){
			String row="row"+i;
			String centroid="center"+i;
			Get g = new Get(Bytes.toBytes(row));
			Result result = table.get(g);
			Put put = new Put(Bytes.toBytes(centroid)); 
			
			put.add(Bytes.toBytes("Area"),Bytes.toBytes("X1"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Area"),Bytes.toBytes("X1")))));
			put.add(Bytes.toBytes("Area"),Bytes.toBytes("X5"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Area"),Bytes.toBytes("X5")))));
			put.add(Bytes.toBytes("Area"),Bytes.toBytes("X6"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Area"),Bytes.toBytes("X6")))));
			put.add(Bytes.toBytes("Area"),Bytes.toBytes("Y1"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Area"),Bytes.toBytes("Y1")))));
			put.add(Bytes.toBytes("Area"),Bytes.toBytes("Y2"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Area"),Bytes.toBytes("Y2")))));
			put.add(Bytes.toBytes("Property"),Bytes.toBytes("X2"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Property"),Bytes.toBytes("X2")))));
			put.add(Bytes.toBytes("Property"),Bytes.toBytes("X3"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Property"),Bytes.toBytes("X3")))));
			put.add(Bytes.toBytes("Property"),Bytes.toBytes("X4"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Property"),Bytes.toBytes("X4")))));
			put.add(Bytes.toBytes("Property"),Bytes.toBytes("X7"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Property"),Bytes.toBytes("X7")))));
			put.add(Bytes.toBytes("Property"),Bytes.toBytes("X8"),Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("Property"),Bytes.toBytes("X8")))));
			
			center.put(put) ; //Insert into the center table
			
		}
		System.out.println("********Center table created*********");


		boolean hasUpdates = true; 
		
			while(hasUpdates){
				Job KMeans = new Job(conf, "KMeans Clustering");
				KMeans.setJarByClass(Program.class);
				Scan scan = new Scan();
				scan.setCaching(500);
				scan.setCacheBlocks(false);  // Can't be true for MR jobs
		
				TableMapReduceUtil.initTableMapperJob(
						"Energy",
						scan, 
						Program.KMeansMapper.class, 
						Text.class, Result.class, KMeans);
		
				TableMapReduceUtil.initTableReducerJob(
						"Center", 
						Program.KMeansReducer.class,
						KMeans);
		
				KMeans.setNumReduceTasks(k); //Number of reduce tasks depends on the k clusters in args[1]
				if (!KMeans.waitForCompletion(true)) {
					return -1;
				}
				//Check the counter value;
				
				long updatedCenter = KMeans.getCounters().findCounter(MyCounters.Counter).getValue();
				System.out.println("Counter was updated by "+updatedCenter+" centers");
				hasUpdates = updatedCenter > 0; //True or False depending on the value in updatedCenter
				System.out.println("*******Running Iteration "+ iteration++ +"*********");
			}

			return 0;
	}
}
