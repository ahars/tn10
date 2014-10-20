package traitementLogs;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.*;

public class InitES {

    public static void main(String[] args) throws IOException {

        Node node = nodeBuilder().clusterName("elasticsearch").node();
        Client client = node.client();

    }
}
