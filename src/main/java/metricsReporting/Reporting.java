package metricsReporting;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class Reporting {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("SparkStreaming")
                .setMaster("local[2]");

        JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(5000));
        System.out.println(sc.sc().getConf().toDebugString());

        //sc.receiverStream(new MetricsReceiver("localhost", 9999)).count().print();

        JavaReceiverInputDStream<String> test = sc.socketTextStream("localhost", 9999);


        ObjectMapper mapper = new ObjectMapper();
        try {
            System.out.println("result = " + mapper.readValue(test.toString(), Map.class));

        } catch (IOException e) {
            e.printStackTrace();
        }

        sc.start();
        sc.awaitTermination();

    }
}
