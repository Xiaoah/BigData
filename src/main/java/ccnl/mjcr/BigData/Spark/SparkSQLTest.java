package ccnl.mjcr.BigData.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class SparkSQLTest {

	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setMaster("local").setAppName("SQL");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(sc);
		
		SparkSession spark = SparkSession
				.builder()
				.appName("SQL")
				.master("local")
				.getOrCreate();
		
		//Dataset<Row> df = spark.read().parquet("users.parquet");
		
		//读取json文件，内容不能有嵌套
		Dataset<Row> df = spark.read().json("people.json");
		df.show();
		
		df.printSchema();
		//System.out.println(df.select("name").count());
		
		/*
		 * select 返回dataset<row>
		 */
		//df.select("age").show();
		//df.select(df.col("name"),df.col("age").plus(10).as("plusAge")).show();
		//df.filter(df.col("age").gt(10)).show();
		
		/*
		 * 按年龄分组统计个数
		 */
		df.groupBy(df.col("age")).count().show();
		
		
		df.createOrReplaceTempView("people_table");
		Dataset<Row> sqlDF = spark.sql("select * from people_table");
		sqlDF.show();
		
		Dataset<Row> pdf = spark.read().json("people.json");
		pdf.write().mode(SaveMode.Overwrite).parquet("people.parquet");
		/*
		 * 这里写入parquet文件的时候报错了，暂时未知原因
		 */
		//pdf.write().format("parquet").mode(SaveMode.Overwrite).save("people.parquet");
		//Dataset<Row> pdf = spark.read().parquet("people.parquet");
		//pdf.show();
		spark.stop();
	}

}
