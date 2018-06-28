package spark_etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class Example4 {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Example3").master("local").getOrCreate();
		
		String logFile = Example4.class.getResource("/testfiles/example3_1.csv").getPath();
		
		testPeopleEncoder(spark, logFile);


		spark.close();

	}

	private static void testPeopleEncoder(SparkSession spark, String logFile) {
		Encoder<People> peopleEncoder = Encoders.bean(People.class);
		Dataset<People> peopleDataFrame = spark.read().option("header", true).csv(logFile).as(peopleEncoder);
		peopleDataFrame.show();
	}

	private static class People {
		String name;
		String sex;
		String age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getSex() {
			return sex;
		}

		public void setSex(String sex) {
			this.sex = sex;
		}

		public String getAge() {
			return age;
		}

		public void setAge(String age) {
			this.age = age;
		}
	}

}
