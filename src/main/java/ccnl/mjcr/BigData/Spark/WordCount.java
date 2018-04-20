package ccnl.mjcr.BigData.Spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * WC统计一个文本里面每一个单词出现的次数
 * @author xiaoah
 *
 */
public class WordCount {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/*
		 * 将文件中的内容加载到linesRDD中
		 */
		JavaRDD<String> linesRDD = sc.textFile("wc",3);
		
		/*
		 * 每一行数据根据空格来分割单词
		 */
		JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
				List<String> list = Arrays.asList(line.split(" "));
				return list.iterator();
			}
		});
		
		/*
		 * 将wordsRDD 中的每一个单词计数为1，变成一个kv格式RDD
		 * 在javaAPI中，想返回一个kv格式的RDD，必须使用mapToPair
		 * pairRDD<k,v> : k：每一个单词，v： 1
		 */
		JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		/*
		 * 使用ReduceByKey进行聚合操作
		 */
		JavaPairRDD<String, Integer> resultRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1 ,Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		/*
		 * 按照单词出现的次数进行排序
		 * 先使用mapToPair来调换位置
		 * 再用sortByKey排序,默认是升序，加上参数false则降序
		 * 再把位置调换回来
		 */
		resultRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				
				return new Tuple2<Integer, String>(tuple._2, tuple._1);
			}
		}).sortByKey().mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
				
				return new Tuple2<String, Integer>(tuple._2, tuple._1);
			}
		}).foreach(new VoidFunction<Tuple2<String,Integer>>() {
		
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple);
			}
		});
		
		sc.stop();
	}

}
