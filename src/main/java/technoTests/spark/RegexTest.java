package technoTests.spark;
/*
import formatLog.ParseFromLogLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Instant;

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


  */
        /*String date_string = "11/Sep/2014:06:26:25 +0200";

        System.out.println(Instant.now());
    }
}
*/
