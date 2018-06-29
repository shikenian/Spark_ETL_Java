/**
 * 测试Spark相关的聚集函数相关的操作。
 * 聚集函数就是我们在SQL语言中对应的Max(),Sum() 等一些针对分组情况下的一些操作函数。
 * Spark本身支持的聚集函数很多，英语中叫做Aggregation
 *
 * 有： sum(),count(), avg(),max(),min() 等等
 *
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
    public static void main(String[] args){

        //首先注册一个Aggregation的类到我们的环境中，方便后期调用
        SparkSession spark = SparkSession.builder().appName("Example3").master("local").getOrCreate();

        String logFile = Example4.class.getResource("/testfiles/example3_1.csv").getPath();
        Encoder<People2> encoder = Encoders.bean(People2.class);

        Dataset<People2> peopleDataFrame = spark.read().option("header", true).option("", "").schema(inputTypes)
                .csv(logFile).as(encoder);
        //接下来做Aggregation操作

        //接下来做数据操作




//        System.out.println("print the schema of Dataset");
//        peopleDataFrame.printSchema();
//        System.out.println("foreach the dataset and print the people name");
//        peopleDataFrame.foreach(e -> System.out.println(e.getName()));

    }

    public static class People2{

        private String name;
        private String sex;
        private int age;

        public People2(){}

        public People2(String name, String sex, int age){
            this.name = name;
            this.sex = sex;
            this.age = age;
        }

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

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    //如果想要实现自主控制对Aggregation的操作，需要基层抽象类UserDefinedAggregateFunction，并且实现里面的每个方法
    public static class myAverage extends UserDefinedAggregateFunction {

        StructType inputSchema = null;
        StructType bufferSchema = null;

        public myAverage(){
            //首先定义好输入的Column信息
            List<StructField> fields = new ArrayList<>();
            fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
            fields.add(DataTypes.createStructField("sex", DataTypes.StringType, false));
            fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
            inputSchema = DataTypes.createStructType(fields);

            //接下来定义输出数据的Column信息
            fields.clear();
            fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
            fields.add(DataTypes.createStructField("sex", DataTypes.StringType, false));
            fields.add(DataTypes.createStructField("avgAge", DataTypes.DoubleType, false));
            bufferSchema = DataTypes.createStructType(fields);

        }

        @Override
        public StructType inputSchema() {
            return inputSchema;
        }

        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        //The data type of returned value
        @Override
        public DataType dataType() {
            return null;
        }

        //Whether this function always returns the same output on the identical input
        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {

        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {

        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

        }

        @Override
        public Object evaluate(Row buffer) {
            return null;
        }
    }

}
