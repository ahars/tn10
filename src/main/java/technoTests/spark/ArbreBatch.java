package technoTests.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ArbreBatch {

    public static void main(String[] args) {

        final String PATH = "/home/ippon/github/sparktacus/src/data/";
        String filename = PATH + "arbresalignementparis2010.csv";

        SparkConf conf = new SparkConf()
                .setAppName("SparkArbreBatch")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO

        sc.stop();
    }
}
