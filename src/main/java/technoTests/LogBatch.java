package technoTests;

import formatLog.ParseFromLogLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

public class LogBatch {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        String filename = PATH + "\\sample.log";
        //String filename = PATH + "\\apache_logs_1.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkLogBatch")
                .setMaster("local")
                .set("spark.files.overwrite", "true")
                .set("spark.serializer", KryoSerializer.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc.getConf().toDebugString());

        sc.textFile(filename)
                .foreach(x -> System.out.println(ParseFromLogLine.apacheAccessLogParse(x).toJSON().string()));
        //sc.textFile(filename).map(ApacheAccessLog::parseFromLogLine).saveAsTextFile(PATH + "output");

        sc.stop();
    }
}
