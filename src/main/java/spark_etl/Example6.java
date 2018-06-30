/**
 * 测试Aggregation函数的另外一种实现方式，指定类型信息的方式。
 * <p>
 * Example5是非指定类型的，因此需要我们在Aggregation类的内部去做类型的指定转换等操作，但是如果使用了下面的方式的，就不需要做这种事情了。
 */

package spark_etl;

import org.apache.spark.Aggregator;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Function2;

import java.io.Serializable;

public class Example6 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Example6").master("local").getOrCreate();
        String logFile = Example6.class.getResource("/testfiles/example3_1.csv").getPath();



//        Encoder

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


        public MyAverage(Function1<Average, Double> createCombiner, Function2<Double, Average, Double> mergeValue, Function2<Double, Double, Double> mergeCombiners) {
            super(createCombiner, mergeValue, mergeCombiners);
        }

        //初始化一个Average
        public Average zero() {
            return new Average(0, 0);
        }

        //用于统计计算源文件的每一行信息，并且存放到缓存中
        public Average reduce(Average buffer, People3 people) {
            buffer.setLineNum(buffer.getLineNum() + 1);
            buffer.setTotalAge(people.getAge() + buffer.getTotalAge());
            return buffer;
        }

        //可能存在多线程的操作，因此需要把结果集合并的操作，这个是底层调用的方法
        public Average merge(Average b1, Average b2) {
            b1.setTotalAge(b2.getTotalAge() + b1.getTotalAge());
            b1.setLineNum(b2.getLineNum() + b1.getLineNum());
            return b1;
        }

        //最后执行的一个操作，是为了能够把Average里面得到的统计信息计算得到最终的结果
        public Double finish(Average reduction) {
            return (double) reduction.getTotalAge() / reduction.getLineNum();
        }

        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }


    }



}
