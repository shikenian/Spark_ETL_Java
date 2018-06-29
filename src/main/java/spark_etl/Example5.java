/**
 * 测试Spark相关的聚集函数相关的操作。
 *
 * 当前类测试的是非类型化的用户定义的聚合函数，也就是Untyped User-Defined Aggregate Functions
 *
 * 聚集函数就是我们在SQL语言中对应的Max(),Sum() 等一些针对分组情况下的一些操作函数。
 * Spark本身支持的聚集函数很多，英语中叫做Aggregation
 * <p>
 * 有： sum(),count(), avg(),max(),min() 等等
 * 手动实现对UserDefinedAggregateFunction的继承来实现对Aggregation操作的个性化实现。
 * 需要实现以下几个方法：
 *
 * inputSchema()
 *  为调用的方法返回一个StructType，告诉我们调用这个Aggregation的时候传递几个参数进来，参数的类型是什么样子的。
 *  i.e. 比如myAverage 这个Aggregation，在调用的时候需要传递进去一个int类型的age信息，看下面的代码这个StructType其实就一个int类型的fields
 *
 * bufferSchema()
 *  告诉调用的上层代码，这个Aggregation对象里面的buffer里面有几个column，并且每个column都是什么类型的。
 *  buffer是这个类计算的中间对象，用来计算并且保存计算的结果。最终会提供给evaluate方法来得到最终的结果。
 * dataType()
 *  告诉上层调用的代码，这个Aggregation最终会返回什么类型的值，一般来说，最终这个Aggregation只会返回一个值，无论你传递进来多少，最终指挥返回一个结果。
 *  而DataType可以告诉我们最终计算的结果是什么类型的。这个是和我们的evaluate方法是对应的，当然如果evaluate方法返回的结果是Object的类型的话，应该最终会被Cast成这个Datatype
 * deterministic()
 *  这个参数还不是很清楚作用，但是目前来看应该是一个开关，告诉Spark在同样的输入结果下，得到的输出结果应该是一致的。但是我不了解的是什么情况下得到的结果才能够不一致。
 * initialize()
 *  用来初始化Buffer，这个Buffer就是用来存储计算的中间变量的。
 * merge()
 *  个人猜测是Spark在计算的时候可能是并行的一种操作，因此的话可能有多个线程，每个线程都持有一个Buffer，但是最终所有并行的结果都要合并到一起做一个汇总，那么这个时候就会调用merge方法。
 *  窄这个merge方法里面我们可以定义一个合并的规则，比如是不是相同就舍弃等等，或者是累加等等。注意，最终计算的结果会被放到第一个参数引用的对象中。这是需要特别注意的地方。
 * evaluate()
 *  取出Buffer中的值，做自己想要的计算得到想要的结果。只会执行一次
 *
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
    public static void main(String[] args) {

        //首先注册一个Aggregation的类到我们的环境中，方便后期调用
        SparkSession spark = SparkSession.builder().appName("Example5").master("local").getOrCreate();
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
