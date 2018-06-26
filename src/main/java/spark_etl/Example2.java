/**
 * 测试：
 * 1：从文本文件建立dataset
 * 2：统计dataset中数据信息
 * 3：使用DataFrame，以表格的方式操作结构化的文件，比如CSV格式的文件。
 */

package spark_etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Example2 {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Example2").master("local").getOrCreate();
		// String logFile =
		// "C:/study/javaprojects/Spark_ETL_Java/src/main/resources/testfiles/example2_1.csv";
		String logFile = "E:/bigdata/projects/Spark_ETL_Java/src/main/resources/testfiles/example2_1.csv";
		Dataset<String> dataset = spark.read().textFile(logFile);

		Dataset<Row> datasetCsv = spark.read().option("header", true).csv(logFile);
		// Dataset<Row> df =
		// spark.read().json("examples/src/main/resources/people.json");

		testWordCount(dataset);

		testShow(datasetCsv);

		spark.close();

	}

	/**
	 * 在java中，DataFrame 其实就是带Row的DataSet DataFrame对结构化的数据操作非常友好，可以像操作一张表一样的对数据进行处理。
	 * DataFrame中有一系列对数据操作的工具和方法，简单来说，有 1：查询 select 2：过滤 filter 3：GroupBy 分组函数
	 */
	private static void testShow(Dataset<Row> dataset) {
		System.out.println("--------------------begin testShow--------------------------");
		System.out.println("打印dataFrame里面列信息");
		dataset.printSchema();
		System.out.println("打印dataFrame里面name列的内容");
		dataset.select("name").show();
		System.out.println("展示所有大于30岁的人");
		dataset.filter(col("age").gt(30)).show();
		System.out.println("把所有人的年龄加一然后展示出来");
		dataset.select(col("age").plus(1)).show();
		System.out.println("--------------------end--------------------------");
	}

	/**
	 * 测试对源文件里面的每一行做一个判断，并且最终判断得到结果
	 * 
	 * @param dataset
	 */
	private static void testWordCount(Dataset<String> dataset) {
		System.out.println("--------------------begin testWordCount--------------------------");
		long numAs = dataset.filter(s -> s.contains("a")).count();
		long numBs = dataset.filter(s -> s.contains("b")).count();

		System.out.println("the number of lines contains a is : " + numAs);
		System.out.println("the number of lines contains b is : " + numBs);

		System.out.println("--------------------end--------------------------");
	}
}
