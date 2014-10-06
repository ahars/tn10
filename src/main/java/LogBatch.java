import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class LogBatch {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        String filename = PATH + "\\sample.log";
        //String filename = PATH + "\\apache_logs_1.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkLogBatch")
                .setMaster("local")
                .set("spark.serializer", KryoSerializer.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile(filename).foreach(x -> System.out.println(ApacheAccessLog.parseFromLogLine(x).toString()));
        sc.textFile(filename).map(ApacheAccessLog::parseFromLogLine).saveAsTextFile(PATH + "output");

        sc.stop();
    }
}
