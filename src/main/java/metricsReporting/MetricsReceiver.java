package metricsReporting;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class MetricsReceiver extends Receiver<Map<String, Object>> {

    String host = null;
    int port = -1;

    public MetricsReceiver(String host, int port) {
        super(StorageLevel.MEMORY_ONLY());
        this.host = host;
        this.port = port;
    }

    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread() {
            @Override
            public void run() {
                receive();
            }
        }.start();
    }

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself isStopped() returns false
    }

    /** Create a socket connection and receive data until receiver is stopped */
    private void receive() {
        // connect to the server
        Socket socket = null;
        String userInput = null;

        ObjectMapper mapper = new ObjectMapper();

        try {
            // connect to the server
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Until stopped or connection broken continue reading
            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data '" + userInput + "'");

                //store(userInput);
                store(mapper.readValue(userInput, Map.class));
            }
            reader.close();
            socket.close();

            // restart in an attempt to connect again when server is active again
            restart("Trying to connect again");

        } catch (ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch (Throwable t) {
            // restart if there is an error during the reading
            restart("Error receiving data", t);
        }
    }

    @Override
    public StorageLevel storageLevel() {
        return null;
    }
}
