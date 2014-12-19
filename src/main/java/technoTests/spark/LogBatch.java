package technoTests.spark;

import formatLog.ParseFromLogLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class LogBatch {

    public static void main(String[] args) {

        final String PATH = "/home/ippon/github/sparktacus/src/data/";
        String filename = PATH + "sample.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkLogBatch")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile(filename).foreach(x -> System.out.println(ParseFromLogLine.logParse(x).toString()));

        sc.stop();
    }
}

