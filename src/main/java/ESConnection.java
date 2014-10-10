import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;

public class ESConnection {

    public static void main(String[] args) {

        Node node = nodeBuilder().clusterName("elasticsearch").node();
        Client client = node.client();

        String log = "" +
                "{" +
                "\"geoip\" : {" +
                "\"ip\" : \"88.162.200.57\"," +
                "\"countryCode\" : \"FR\"," +
                "\"countryName\" : \"France\"," +
                "\"region\" : \"A8\"," +
                "\"regionName\" : \"Ile-de-France\"," +
                "\"city\" : \"Paris\"," +
                "\"postalCode\" : \"75010\"," +
                "\"lnglat\" : [2.3560944, 48.870895]," +
                "\"latitude\" : \"48.870895\"," +
                "\"longitude\" : \"2.3560944\"," +
                "\"metroCode\" : \"0\"," +
                "\"areaCode\" : \"0\"," +
                "\"timezone\" : \"Europe/Paris\"" +
                "}" +
                "}";

        IndexResponse response = client.prepareIndex("sparky", "connection", "1")
                .setSource(log)
                .execute()
                .actionGet();

        System.out.println(response.getIndex());

        //node.close();
    }
}
