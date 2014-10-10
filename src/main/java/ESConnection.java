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
                "}," +
                "\"clientID\" : \"-\"," +
                "\"userID\" : \"-\"," +
                "\"dateLog\" : {" +
                "\"dateTimeString\" : \"11/Sep/2014:06:29:24 +0200\"," +
                "\"timestamp\" : 1410409764000," +
                "\"day\" : 4," +
                "\"date\" : 11," +
                "\"month\" : 8," +
                "\"year\" : 2014," +
                "\"hours\" : 6," +
                "\"minutes\" : 29," +
                "\"seconds\" : 24," +
                "\"timezonOffset\" : -120" +
                "}," +
                "\"method\" : \"GET\"," +
                "\"endPoint\" : \"/lib/php/fonction/download.php?enseigne=2352&export-produit-pdf=\"," +
                "\"protocol\" : {" +
                "\"nom\" : \"HTTP\"," +
                "\"version\" : \"1.1\"" +
                "}," +
                "\"responseCode\" : 200," +
                "\"contentSize\" : 3093," +
                "\"link\" : \"http://manager.skytill.fr/index.php\"," +
                "\"mozillaVersion\" : {" +
                "\"nom\" : \"Mozilla\"," +
                "\"version\" : \"5.0\"" +
                "}," +
                "\"os\" : {" +
                "\"type\" : \"Macintosh\"," +
                "\"nom\" : \"Intel Mac OS X\"," +
                "\"version\" : \"10_9_3\"" +
                "}," +
                "\"kit\" : {" +
                "\"type\" : \"AppleWebKit\"," +
                "\"version\" : \"537.36\"" +
                "}," +
                "\"renduHtml\" : {" +
                "\"nom\" : \"KHTML\"," +
                "\"type\" : \"Gecko\"" +
                "}," +
                "\"chromeVersion\" : {" +
                "\"nom\" : \"Chrome\"," +
                "\"version\" : \"37.0.2062.94\"" +
                "}," +
                "\"safariVersion\" : {" +
                "\"nom\" : \"Safari\"," +
                "\"version\" : \"537.36\"" +
                "}" +
                "}";

        IndexResponse response = client.prepareIndex("connexion", "log", "1")
                .setSource(log)
                .execute()
                .actionGet();

        System.out.println(response.getIndex());

        node.close();
    }
}
