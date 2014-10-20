package technoTests;

import formatLog.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class LogWriteToES {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        String filename = PATH + "\\sample.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkToES")
                .setMaster("local")
                .set("es.nodes", "10.10.200.249:9200")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("es.index.auto.create", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc.getConf().toDebugString());

        JavaRDD<String> javaRDD = sc.textFile(filename).map(x -> ApacheAccessLog.parseFromLogLine(x).toString());
        System.out.println(javaRDD.first().toString());

        Node node = nodeBuilder().clusterName("elasticsearch").node();
        Client client = node.client();

        IndexResponse response = client.prepareIndex("sparky", "LogWrite")
                .setSource(javaRDD.first().toString())
                .execute()
                .actionGet();

        System.out.println(response.getIndex());

        node.close();
        sc.stop();
    }
}

