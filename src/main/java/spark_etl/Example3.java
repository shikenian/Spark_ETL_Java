/**
 * 像操作SQL那样操作DataFrame
 * 在这里我们可以像数据中定义视图一样定义一个View，View一共分为两种作用域：
 * 	TempView：作用域为当前session，只有当前session中才可以读取view中数据
 * 	GlobalView：作用域为所有session，可以跨session读取view中的数据
 */

package spark_etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Example3 {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Example3").master("local").getOrCreate();
		String logFile = Example1.class.getResource("/testfiles/example3_1.csv").getPath();

		//设置指定CSV文件的第一行为表头，这一步先建立好表结构之类的
		Dataset<Row> datasetCsv = spark.read().option("header", true).csv(logFile);
		
		//测试临时视图
		TestTempView(spark, datasetCsv);
		
		//测试全局视图
		TestGlobalView(spark, datasetCsv);
		
		spark.close();
		
		
	}

	private static void TestGlobalView(SparkSession spark, Dataset<Row> datasetCsv) {
		System.out.println("测试全局视图");
		
		datasetCsv.createOrReplaceGlobalTempView("employee");
		//注意，读取global的view，需要在前面指定global_temp前缀。全局view和临时view是可以重名的
		//这里通过当前的SparkSession，开启一个新的SparkSession，并且使用新的session来执行spark sql
		Dataset<Row> sqlDF = spark.newSession().sql("select * from global_temp.employee");
		sqlDF.show();
	}

	private static void TestTempView(SparkSession spark, Dataset<Row> datasetCsv) {
		System.out.println("测试临时视图");
		//指定一个视图，把视图定义存入到上下文中
		datasetCsv.createOrReplaceTempView("employee");
		
		//定义一个SQL Query，并且使用spark SQL来调用和执行
		Dataset<Row> sqlDF = spark.sql("select * from employee");
		
		sqlDF.show();
	}	
}
