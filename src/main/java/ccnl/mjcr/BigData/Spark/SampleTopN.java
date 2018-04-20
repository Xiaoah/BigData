package ccnl.mjcr.BigData.Spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SampleTopN {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("topN");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> linesRDD = sc.textFile("score");
		JavaPairRDD<String, Integer> pairRDD = linesRDD.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] splited = line.split(" ");
				String className = splited[0];
				Integer score = Integer.valueOf(splited[1]);
	
				return new Tuple2<String, Integer>(className, score);
			}
		});
		
		pairRDD.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				String className = tuple._1;
				Iterator<Integer> iterator = tuple._2.iterator();
				Integer[] top3 = new Integer[3];
				
				while(iterator.hasNext()) {
					Integer score = iterator.next();
					for(int i=0;i<top3.length;i++) {
						if(top3[i] == null) {
							top3[i] = score;
							break;
						}
						if(score>top3[i]) {
							for(int j=top3.length-1;j>i;j--) {
								top3[j] = top3[j-1];
							}
							top3[i] = score;
							break;
						}
					}
				}
				System.out.println("className:"+className);
				for(Integer sscore:top3) {
					System.out.print(sscore+" ");
				}
			}
		});
		
		
	}

}
