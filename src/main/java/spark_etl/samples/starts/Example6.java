/**
 * 测试Aggregation函数的另外一种实现方式，类型安全的实现方式。
 * <p>
 * Example5是非指定类型的，因此需要我们在Aggregation类的内部去做类型的指定转换等操作。
 * 如果我们制定了Buffer处理的类型的话,可以更加简单实现Aggregation操作，因为底层会帮我们把结果封装到对象中。
 * 我们已知的是，Dataset中对象的存在方式是以字节码的方式存在的，所以相对来说比会比较节省空间。
 * 这对于处理大批量的数据来说是一个比较重要的特性。
 *
 */

package spark_etl;

import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Example6 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Example6").master("local").getOrCreate();
        String logFile = Example6.class.getResource("/testfiles/example3_1.csv").getPath();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("sex", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
        StructType structType = DataTypes.createStructType(fields);
        Encoder<People3> encoder = Encoders.bean(People3.class);
        Dataset<People3> peopleDataFrame = spark.read().option("header", true).schema(structType).csv(logFile).as(encoder);

        MyAverage myAverage = new MyAverage();

        Dataset<Double> dataResult = peopleDataFrame.select(myAverage.toColumn().name("average_age"));

        System.out.println("Print the result schema");
        dataResult.printSchema();
        System.out.println("print the data result");
        dataResult.show();


    }

    //People3 是用来从原文件读取内容的
    public static class People3 implements Serializable {
        private String name;
        private String sex;
        private int age;

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

    //Average是用来缓存源文件里面的一些计算结果，并且缓存的结算结果会被拿来继续使用
    public static class Average implements Serializable {
        public Average(int lineNum, int totalAge) {
            this.lineNum = lineNum;
            this.totalAge = totalAge;
        }

        public Average() {
        } //需要有个无参数的额构造函数

        private int lineNum;
        private int totalAge;

        public int getLineNum() {
            return lineNum;
        }

        public void setLineNum(int lineNum) {
            this.lineNum = lineNum;
        }

        public int getTotalAge() {
            return totalAge;
        }

        public void setTotalAge(int totalAge) {
            this.totalAge = totalAge;
        }

    }

    //具体的Aggregation方法，继承Aggregator类
    public static class MyAverage extends Aggregator<People3, Average, Double> {


        //初始化一个Average
        @Override
        public Average zero() {
            return new Average(0, 0);
        }

        //用于统计计算源文件的每一行信息，并且存放到缓存中，类似Hadoop中的Map操作
        @Override
        public Average reduce(Average buffer, People3 people) {
            buffer.setLineNum(buffer.getLineNum() + 1);
            buffer.setTotalAge(people.getAge() + buffer.getTotalAge());
            return buffer;
        }

        //可能存在多线程的操作，因此需要把结果集合并的操作，这个是底层调用的方法
        @Override
        public Average merge(Average b1, Average b2) {
            b1.setTotalAge(b2.getTotalAge() + b1.getTotalAge());
            b1.setLineNum(b2.getLineNum() + b1.getLineNum());
            return b1;
        }

        //最后执行的一个操作，是为了能够把Average里面得到的统计信息计算得到最终的结果
        @Override
        public Double finish(Average reduction) {
            return (double) reduction.getTotalAge() / reduction.getLineNum();
        }

        //该方法是用来告诉上层调用当前的Buffer的类型是什么样子
        @Override
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        //该方法是告诉上层调用输出的结果是什么类型
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }


    }


}
