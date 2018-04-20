package ccnl.mjcr.BigData.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SecondSortTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("secondsort");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> second = sc.textFile("secondsortfile"); 
		JavaPairRDD<SecondSortKey,String> pairSecondRDD = second.mapToPair(new PairFunction<String, SecondSortKey, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<SecondSortKey, String> call(String line) throws Exception {
				String[] splited = line.split(" ");
				int first = Integer.valueOf(splited[0]);
				int second = Integer.valueOf(splited[1]);
				SecondSortKey secondSortKey = new SecondSortKey(first,second);
				return new Tuple2<SecondSortKey,String>(secondSortKey,line);
			}
		});
		
		pairSecondRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<SecondSortKey,String>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<SecondSortKey, String> tuple) throws Exception {
				System.out.println(tuple._2);
				
			}
		});
	}

}
