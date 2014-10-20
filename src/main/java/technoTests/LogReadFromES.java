package technoTests;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class LogReadFromES {

    public static void main(String[] args) {

        Node node = nodeBuilder().clusterName("elasticsearch").node();
        Client client = node.client();

        GetResponse response = client.prepareGet("sparky", "LogWrite", "1")
                .execute()
                .actionGet();

        System.out.println(response.getSourceAsString());

        node.close();
    }
}

