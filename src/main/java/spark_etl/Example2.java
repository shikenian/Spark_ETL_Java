/**
 * 测试：
 * 1：从文本文件建立dataset
 * 2：统计dataset中数据信息
 * 3：使用DataFrame，以表格的方式操作结构化的文件，比如CSV格式的文件。
 * 4：
 */


package spark_etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;


public class Example2 {
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Example2").master("local").getOrCreate();
		String logFile = "C:/study/javaprojects/Spark_ETL_Java/src/main/resources/testfiles/example2_1.csv";
		Dataset<String> dataset = spark.read().textFile(logFile);
		
		Dataset<Row> datasetCsv = spark.read().option("header", true).csv(logFile);
		//Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");
		
		testWordCount(dataset);
		
		testShow(datasetCsv);
		
		spark.close();
		
		
	}

	private static void testShow(Dataset<Row> dataset) {
		System.out.println("--------------------begin testShow--------------------------");
		System.out.println("print the dataset schema");
		dataset.printSchema();
		System.out.println("print one column of dataset");
		dataset.select("name").show();
		System.out.println("select every body but do some operate on the head");
		dataset.select(col("name").alias("simplename")).show();
		
		System.out.println("--------------------end--------------------------");
	}

	/**
	 * 测试对源文件里面的每一行做一个判断，并且最终判断得到结果
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
