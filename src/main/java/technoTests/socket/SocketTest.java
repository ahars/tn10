package technoTests.socket;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.*;
import java.net.Socket;

public class SocketTest {

    public static void main(String[] args) {

        Socket socket;
        String host = "localhost";
        Integer port = 9999;

        try {
            socket = new Socket(host, port);

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);

            String data = "YO";
            toServer.println(data);
            System.out.println("client has send : " + data);

            String response = fromServer.readLine();
            System.out.println("client has received : " + response);

            JsonGenerator json = new JsonFactory().createGenerator(socket.getOutputStream());
            json.writeStartObject();
            json.writeStringField("name", "name");
            json.writeStringField("type", "timer");
            json.writeNumberField("timestamp", 1234567890);
            json.writeNumberField("count", 12);
            json.writeNumberField("m1_rate", 24);
            json.writeNumberField("m5_rate", 36);
            json.writeNumberField("m15_rate", 48);
            json.writeNumberField("mean_rate", 52);
            json.writeNumberField("max", 64);
            json.writeNumberField("mean", 76);
            json.writeNumberField("min", 88);
            json.writeNumberField("p50", 90);
            json.writeNumberField("p75", 128);
            json.writeNumberField("p95", 216);
            json.writeNumberField("p98", 512);
            json.writeNumberField("p99", 1024);
            json.writeNumberField("p999", 2056);
            json.writeNumberField("stddev", 4000);
            json.writeEndObject();

            json.flush();
            System.out.println("send data : " + json.toString());

        } catch (IOException e) {
            System.out.println("Critic failure : " + e);
        }

    }
}
