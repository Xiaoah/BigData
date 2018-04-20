package ccnl.mjcr.BigData.Spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class DistinctOpera {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("app").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String,Integer>> list = Arrays.asList(
				new Tuple2<String,Integer>("a",1),
				new Tuple2<String,Integer>("b",2),
				new Tuple2<String,Integer>("b",2)
				);
		/*
		 * 在java api中，如果要转换为k-v格式的rdd，需要用到parallizePairs
		 */
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
		
		/*
		 * (("a",1),1)
		 */
		JavaPairRDD<Tuple2<String,Integer>,Integer> pairRDD = rdd.mapToPair(new PairFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Integer>() {

			/**
			 * 实现序列化
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<Tuple2<String, Integer>, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				
				return new Tuple2<Tuple2<String,Integer>,Integer>(tuple,1);
			}
		});
			
		pairRDD.groupByKey().foreach(new VoidFunction<Tuple2<Tuple2<String,Integer>,Iterable<Integer>>>() {

			/**
			 * 实现序列化
			 */
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<Tuple2<String, Integer>, Iterable<Integer>> tuple) throws Exception {
				System.out.println(tuple._1);
			}
		});
		
		/**
		 * 非KV格式的rdd
		 */
		List<String> list1 = Arrays.asList(
				"hello",
				"world",
				"good",
				"hello"
				);
		/*
		 *注意该方法与k-v格式的不同 
		 */
		JavaRDD<String> rdd1 = sc.parallelize(list1);
		JavaPairRDD<String, Integer> pairRDD1 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String str) throws Exception {
				return  new Tuple2<String,Integer>(str,1);
			}
		});
		
		pairRDD1.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

		
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				System.out.println(tuple._1);
			}
			
		});
		
		sc.stop();
	} 

}
