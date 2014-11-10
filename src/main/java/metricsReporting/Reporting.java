package metricsReporting;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.actors.threadpool.Arrays;

public class Reporting {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("SparkStreaming")
                .setMaster("local[2]");

        JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(2000));
        System.out.println(sc.sc().getConf().toDebugString());

        //JavaDStream<String> customReceiverStream = sc.receiverStream(new MetricsReceiver("127.0.0.1", 9999));
        sc.socketTextStream("localhost", 9999).count().print();

        sc.start();
        sc.awaitTermination();
    }
}
