package ccnl.mjcr.BigData.Spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DatasetCreate {

	public static class Person implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String name;
		private int age;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		
	}

	private static final int MapFunction = 0;
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()
										.appName("datasetCreate")
										.master("local")
										.getOrCreate();
	
		
		/*
		 * 创建一个bean类
		 */
		Person person = new Person();
		person.setAge(12);
		person.setName("xiaoah");
		
		/*
		 * 为java bean 编码
		 */
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();
		
		/*
		 * 公共类型的encoder
		 */
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1,2,3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer,Integer>) value -> value+1
			, integerEncoder);
		transformedDS.show();
		
		/*
		 * 动态生成dataset
		 */
		
		JavaRDD<String> peopleRDD = spark.sparkContext().textFile("people.txt", 1).toJavaRDD();
		String schemaString = "name age";
		List<StructField> fields = new ArrayList<>();
		for(String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		
		//convert records of peopleRDD to Rows
		JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
			String[] attributes = record.split(",");
			return RowFactory.create(attributes[0],attributes[1].trim());
		});
		
		//apply the schema to the rdd
		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
		peopleDataFrame.createOrReplaceTempView("pl");
		Dataset<Row> sqlresult = spark.sql("select * from pl");
		sqlresult.show();
		
		spark.stop();
	}

}
