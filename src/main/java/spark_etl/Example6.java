/**
 * 测试Aggregation函数的另外一种实现方式，指定类型信息的方式。
 * <p>
 * Example5是非指定类型的，因此需要我们在Aggregation类的内部去做类型的指定转换等操作，但是如果使用了下面的方式的，就不需要做这种事情了。
 */

package spark_etl;

import org.apache.spark.Aggregator;
import org.apache.spark.sql.Encoder;
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

    public static class People3 implements Serializable {
        private String name;
        private String sex;
        private int age;
    }

    public static class Average implements Serializable {
        private int lineNum;
        private int average;
    }

    public static class MyAverage extends Aggregator<People3, Average, Double> {


        public MyAverage(Function1<Average, Double> createCombiner, Function2<Double, Average, Double> mergeValue, Function2<Double, Double, Double> mergeCombiners) {
            super(createCombiner, mergeValue, mergeCombiners);
        }

        public Average zero() {
            return new Average(0, 0);
        }
    }
}
