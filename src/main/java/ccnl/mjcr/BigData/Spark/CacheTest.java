package ccnl.mjcr.BigData.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class CacheTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("cache");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//设置checkpoint的存储路径
		//sc.setCheckpointDir("D:/checkpoint");
		
		JavaRDD<String> linesRDD = sc.textFile("wc");
		
		/*
		 * 持久化到内存
		 */
		linesRDD = linesRDD.cache();
		//linesRDD = linesRDD.persist(StorageLevel.MEMORY_ONLY());
		
		/*
		 * 设置检查点
		 */
		//linesRDD.checkpoint();
		
		long startTime1 = System.currentTimeMillis();
		long count1 = linesRDD.count();
		long endTime1 = System.currentTimeMillis();
		System.out.println("count: "+count1+" time: "+(endTime1-startTime1));
		
		long startTime2 = System.currentTimeMillis();
		long count2 = linesRDD.count();
		long endTime2 = System.currentTimeMillis();
		System.out.println("count: "+count2+" time:"+(endTime2-startTime2));
		
		sc.stop();
	}
}