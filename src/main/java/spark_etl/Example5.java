/**
 * 测试Spark相关的聚集函数相关的操作。
 * 聚集函数就是我们在SQL语言中对应的Max(),Sum() 等一些针对分组情况下的一些操作函数。
 * Spark本身支持的聚集函数很多，英语中叫做Aggregation
 * <p>
 * 有： sum(),count(), avg(),max(),min() 等等
 */


package spark_etl;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Example5 {
    public static void main(String[] args) {

        //首先注册一个Aggregation的类到我们的环境中，方便后期调用
        SparkSession spark = SparkSession.builder().appName("Example3").master("local").getOrCreate();
        spark.udf().register("myAverage", new MyAverage()); //UDF 方法提供接口让我们定义自己的Aggregation函数
        //查找Source文件，并且定义好Encoder方便后期操作
        String logFile = Example5.class.getResource("/testfiles/example3_1.csv").getPath();

        //建立一个TempView方便后期操作
        Dataset<Row> peopleDataFrame = spark.read().option("header", true).csv(logFile);//.as(encoder);//as 操作知识把Encoder中的对应Column名称给映射上去，并没有改变类型信息，类型信息还是得靠StructType来改变
        peopleDataFrame.createOrReplaceTempView("people2");
        System.out.println("展示当前DataFrame里面所有的数据信息");
        peopleDataFrame.show();
        System.out.println("使用Pysql调用自定义的Aggregation方法");
        //接下来做Aggregation操作
        Dataset<Row> result = spark.sql("select myAverage(age) as avgAge from  people2");
        //接下展示数据
        System.out.println("最终Aggregation的结果是：");
        result.show();



    }

    //如果想要实现自主控制对Aggregation的操作，需要基层抽象类UserDefinedAggregateFunction，并且实现里面的每个方法
    public static class MyAverage extends UserDefinedAggregateFunction {

        StructType inputSchema = null;
        StructType bufferSchema = null;

        public MyAverage() {
            //首先定义好输入的Column信息,如果只有一个column的话，那么只会传递一个值给这个对象
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
            inputSchema = DataTypes.createStructType(inputFields);

            //定义缓冲区的存放的值的信息，缓冲区的值最终是会被拿来执行计算得到最终结果的
            List<StructField> BufferFields = new ArrayList<>();
            BufferFields.clear();
            BufferFields.add(DataTypes.createStructField("linesNum", DataTypes.IntegerType, false));
            BufferFields.add(DataTypes.createStructField("TotalAge", DataTypes.IntegerType, false));
            bufferSchema = DataTypes.createStructType(BufferFields);

        }

        //提供对源文件的Schema
        @Override
        public StructType inputSchema() {
            return inputSchema;
        }

        //提供把数据导入到Buffer的schema
        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        //最终这个类会返回的结果类型信息，请对照下面的evaluate方法的返回值
        @Override
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        //Whether this function always returns the same output on the identical input
        //设置是否对相同的输入能够有相同的输出
        @Override
        public boolean deterministic() {
            return true;
        }

        //初始化Buffer，只调用一次
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0);
            buffer.update(1, 0);
        }

        //对每一行数据执行一个操作，一个Row就是一行数据，会根据InputSchema把数据转换成相关的类型
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            //定义输出的内容
            buffer.update(0, buffer.getInt(0) + 1);
            buffer.update(1, buffer.getInt(1) + input.getInt(0));
        }

        //个人猜测是因为处理的过程是并行的，所以可能会有多个Buffer存在，但是最终的结果是要汇总的，所以merge方法的作用是把
        //多线程的结果进行汇总
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            int lineNumber = buffer1.getInt(0) + buffer2.getInt(0);
            int totalAge = buffer1.getInt(1) + buffer2.getInt(1);
            buffer1.update(0, lineNumber);
            buffer1.update(1, totalAge);
        }

        //最终对buffer里面的内容进行计算，应该只执行一次
        @Override
        public Double evaluate(Row buffer) {
            return (double) (buffer.getInt(1) / buffer.getInt(0));
        }
    }

}
