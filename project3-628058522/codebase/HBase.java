import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

public class HBase {

	@SuppressWarnings("deprecation")
	//Function used to Insert values into the rows 
	public static void InsertRow(Put put, List<Double> center_values) {
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X1"),Bytes.toBytes(center_values.get(0).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X5"),Bytes.toBytes(center_values.get(1).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X6"),Bytes.toBytes(center_values.get(2).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("Y1"),Bytes.toBytes(center_values.get(3).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("Y2"),Bytes.toBytes(center_values.get(4).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X2"),Bytes.toBytes(center_values.get(5).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X3"),Bytes.toBytes(center_values.get(6).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X4"),Bytes.toBytes(center_values.get(7).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X7"),Bytes.toBytes(center_values.get(8).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X8"),Bytes.toBytes(center_values.get(9).toString()));
		
	}

	@SuppressWarnings("deprecation")
	
	//EucDistance is used to calculate the distance between the Old_Center and New_Center
	public static Double EucDistance(List<Double> val1, KeyValue[] val2) {
		if (val1.size() != val2.length) {
			System.out.println("NOT EQUAL LENGTHs");
			return Double.MAX_VALUE;
		}
		int length = val2.length;
		Double dev = 0D;
		
		for (int i = 0; i < length; i++) {
			Double value2 = new Double(Bytes.toString(val2[i].getValue()));
			dev += Math.pow(val1.get(i) - value2, 2);
		}
		
		return Math.sqrt(dev / length);
	}
	
	@SuppressWarnings("deprecation")
	
	//Used to calculate the distance of the datapoints from the centers for grouping
	public static Double EucDistance(KeyValue[] val1, KeyValue[] val2) {
		if (val1.length != val2.length) {
			System.out.println("NOT EQUAL LENGTHs");
			return Double.MAX_VALUE;
		}
		int length = val1.length;
		Double dev = 0D;

		for (int i = 0; i < length; i++) {
			Double V1 = new Double(Bytes.toString(val1[i].getValue()));
			Double V2 = new Double(Bytes.toString(val2[i].getValue()));
			dev += Math.pow(V1 - V2, 2);

		}
		return Math.sqrt(dev/length) ;
	}
	
	@SuppressWarnings({ "deprecation", "resource" })
	//This is used in the setup function of Mappers and Reducers to load values from the center table
	public static Map<String, KeyValue[]> LoadValue(String tableName) throws IOException {
		Map<String, KeyValue[]> centers = new HashMap<String, KeyValue[]>();
		Configuration config = HBaseConfiguration.create();
		HTable currentCenter = new HTable(config, tableName);
		Scan scc = new Scan();
		ResultScanner scanner =  currentCenter.getScanner(scc);
			
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			String key = Bytes.toString(rr.getRow());
			KeyValue[] values = rr.raw();
			System.out.println(key+" "+Bytes.toString(rr.getValue(Bytes.toBytes("Area"),Bytes.toBytes("X2"))));
			centers.put(key, values);
		}
		return centers;
	}
	
	
	@SuppressWarnings("rawtypes")
	//Used to calculate the NearestCenter for the clustering 
	public static String NearestCenter(Map<String, KeyValue[]> centers, KeyValue[] point) {
		String nId = "";
		Double distance_val = Double.MAX_VALUE;
	
		Iterator<Entry<String, KeyValue[]>> it = centers.entrySet().iterator();
		while(it.hasNext()){
			
			Map.Entry pair = (Map.Entry)it.next();
			Double dis = EucDistance((KeyValue[])pair.getValue(),point);  //finding the distance of a point from the k centers 
			
			if (dis < distance_val) {
				distance_val = dis;
				nId = (String) pair.getKey();
			}
		}	
		return nId;
	}
	
	@SuppressWarnings("deprecation")
	
	//Used to calculate the new center values
	public static List<Double> NewMean(Iterable<Result> results) {
		int total = 0;
		int pos=0 ; 
		Multimap<Integer,Double> PosVal = ArrayListMultimap.create();	
		List<Integer> Positions = new ArrayList<Integer>();
		List<Double> PositionalMean = new ArrayList<Double>();
		for (Result r : results) {
			total++ ;
		
			KeyValue[] values = r.raw();
			String a = Bytes.toString(r.getRow()) ;
			String row = a.replaceFirst(".*?(\\d+).*", "$1");
			if(!Positions.contains(Integer.parseInt(row)))
				Positions.add(Integer.parseInt(row));
			
			for(int i =0;i<values.length;i++){
				PosVal.put(Integer.parseInt(row), new Double(Bytes.toString(values[i].getValue())));
			}		

		}		
		for(int j =0 ; j<10;j++){
			double sum = 0 ; 
			for(int i=0;i<Positions.size();i++)
			{	
				int a = Positions.get(i) ; 
				List<Double> val = (List<Double>) PosVal.get(a);
				sum+=val.get(j);
			}
			PositionalMean.add(j,sum/Positions.size()) ; 		
		}
		return PositionalMean;

	}
}
	
		
			
