import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.*;

public class ESConnection {

    public static void main(String[] args) throws IOException {

        Node node = nodeBuilder().clusterName("elasticsearch").node();
        Client client = node.client();

        String log = "" +
                "{" +
                "\"name\" : \"Sparky\"" +
                "}";

        IndexResponse response = client.prepareIndex("sparky", "connection", "1")
                .setSource(log)
                .execute()
                .actionGet();

        System.out.println(response.getIndex());


        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", "12")
                .field("message", "trying out Elasticsearch")
                .endObject();
        System.out.println(builder.string());

        IndexResponse ree = client.prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", "12")
                    .field("message", "trying out Elasticsearch")
                    .endObject()
                )
                .execute()
                .actionGet();
    }
}
