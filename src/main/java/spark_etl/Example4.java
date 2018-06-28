/**
 * Spark Encoder 的测试
 * 
 * 在Spark的Dataset 和DataFrame中，我们的类其实是以二进制的方式存放在里面。不同于我们Java官方序列化的二进制文件，Spark提供了Encoder类对对象进行序列化，
 * 并且Spark可以直接操作这些二进制文件对象，而不需要把二进制文件反序列化成Java对象。这点是Java官方的序列化方式所不能达到的。
 * 
 * 使用一个对象保存一行文件内容的话，我觉得好处应该是可以更加方便的对这个数据的类型和长度等信息做出判断，但是也有一些坏处，可能对内存的空间占用量会更加的高。
 * 这个是比较难以判断的暂时，我觉得需要通过阅读源代码才能够了解更加多更加详细的情况
 * 
 * 测试通过一个list构建一个dataset对象，
 * 测试通过一个文件构建一个dataset对象
 * 测试通过一个RDD构建一个dataset对象
 */

package spark_etl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Example4 {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Example3").master("local").getOrCreate();

		String logFile = Example4.class.getResource("/testfiles/example3_1.csv").getPath();

		Encoder<People> encoder = Encoders.bean(People.class);
		// 读取文件成为一个Dataset，并且使用Encoder把每行内容转换为对象
		testPeopleEncoder(spark, encoder, logFile);

		// 从一个List转换成为一个Dataset，这个过程需要Encoder对象的支持
		testPeopleEncoderWithList(spark, encoder);

		testRddToDataset(spark, encoder, logFile);

		spark.close();

	}

	/**
	 * 把一个RDD转换成为一个Dataset
	 * 
	 * @param spark
	 * @param logFile
	 * @param encoder
	 */
	private static void testRddToDataset(SparkSession spark, Encoder<People> encoder, String logFile) {
		// Create a RDD base on the source file
		JavaRDD<People> rdd = spark.read().option("header", true).textFile(logFile).javaRDD().map(line -> {
			String[] parts = line.split(",");
			if (!parts[2].matches("^[0-9]*$")) {
				return null;
			}
			return new People(parts[0], parts[1], Integer.parseInt(parts[2]));
		}).filter(p -> p != null);
		// Crate a dataset base on the RDD,这只是简单的一个dataset，可以做一些简单的操作。
		Dataset<Row> dataset = spark.createDataFrame(rdd, People.class);

		// 在简单的dataset的基础上做一个新的Dataset，支持对People的遍历
		Dataset<People> peopleDataset = dataset.as(encoder);
		peopleDataset.foreach(e -> {
			System.out.println("Print peopleDataset, name is : " + e.getName());
		});
	}

	/**
	 * 通过List手动构建一个Dataset对象
	 * 
	 * @param spark
	 */
	private static void testPeopleEncoderWithList(SparkSession spark, Encoder<People> encoder) {
		People pl = new People("kshi", "male", 32);

		Dataset<People> dataset = spark.createDataset(Collections.singletonList(pl), encoder);

		dataset.show();
	}

	/**
	 * 通过一个文件构建一个Dataset对象
	 * 使用Encoder对象后，可以把文件的每一行封装到一个对象中，这个时候就可以对这个对象进行长度的控制等操作。当然，如果是大文件，
	 * 我想这个时候得考虑到对象太多是否会占用太多的内存空间的问题。
	 * 
	 * 当读取一个文件的时候，需要为这个文件设置一个schema，那么需要在DataFrameReader的方法中进行设置
	 * 当然如果从一个RDD转换成个DataFrame的时候，也可以直接把StructType类传递过去，创建一个RDD
	 * 
	 * @param spark
	 * @param logFile
	 */
	private static void testPeopleEncoder(SparkSession spark, Encoder<People> encoder, String logFile) {

		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
		fields.add(DataTypes.createStructField("sex", DataTypes.StringType, false));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
		StructType structType = DataTypes.createStructType(fields);

		Dataset<People> peopleDataFrame = spark.read().option("header", true).option("", "").schema(structType)
				.csv(logFile).as(encoder);

		System.out.println("print the schema of Dataset");
		peopleDataFrame.printSchema();
		System.out.println("foreach the dataset and print the people name");
		peopleDataFrame.foreach(e -> System.out.println(e.getName()));
	}

	/**
	 * 如果静态内部类是private的话，会导致当这个类的对象被传递给外部的一些类调用的时候，无法被使用。
	 * 如果只是在当前主类中内部调用还没问题，但是如果是被外部调用的话总是会出现各种奇怪的运行时异常 特别是我遇到过多次private类型的类被Spark
	 * 调用的时候，总是出现的各种奇怪的问题
	 * 
	 * @author kshi
	 *
	 */
	public static class People {
		private String name;
		private String sex;
		private int age;

		public People() {
		}

		public People(String name, String sex, int age) {
			this.name = name;
			this.sex = sex;
			this.age = age;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @return the sex
		 */
		public String getSex() {
			return sex;
		}

		/**
		 * @return the age
		 */
		public int getAge() {
			return age;
		}

		/**
		 * @param name
		 *            the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @param sex
		 *            the sex to set
		 */
		public void setSex(String sex) {
			this.sex = sex;
		}

		/**
		 * @param age
		 *            the age to set
		 */
		public void setAge(int age) {
			this.age = age;
		}

	}

}
