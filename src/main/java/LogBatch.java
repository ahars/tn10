import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LogBatch {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Google Drive\\tn10\\src\\data\\";
        String filename = PATH + "\\apache_logs_1.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkLogBatch")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> logLines = sc.textFile(filename);
        System.out.println(logLines.first().toString());

        JavaRDD<ApacheAccessLog> logData = logLines.map(ApacheAccessLog::parseFromLogLine).cache();
        System.out.println(logData.first().getChromeVersion());
        System.out.println(logData.first().getSafariVersion());

        sc.stop();
    }
}
