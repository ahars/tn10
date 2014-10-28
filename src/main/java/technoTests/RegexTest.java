package technoTests;

import formatLog.ParseFromLogLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class RegexTest implements ParseFromLogLine {

    public static void main(String[] args) {

        final String PATH = "/Users/ahars/sparky/src/data/";
        String filename = PATH + "/sample.log";

        SparkConf conf = new SparkConf()
                .setAppName("Regex")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc.getConf().toDebugString());
/*
        String _1 = sc.textFile(filename).first();
        if (_1.matches(APACHE_ACCESS_LOG_PATTERN)) {
            System.out.println(ParseFromLogLine.apacheAccessLogParse(_1).toString());
        } else {
            System.out.println("logFormat non reconnu");
        }
  */  }
}
