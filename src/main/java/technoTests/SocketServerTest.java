package technoTests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServerTest {

    public static void main(String[] args) {

        try {
            ServerSocket server = new ServerSocket(9999);
            Socket client = server.accept();

            BufferedReader fromClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
            PrintWriter toClient = new PrintWriter(client.getOutputStream(), true);

            String response = fromClient.readLine();
            System.out.println("server has received : " + response);
            String data = "THX";
            toClient.println(data);
            System.out.println("server has send : " + data);

            System.out.println("server has received : " + fromClient.readLine());

        } catch (IOException e) {
            System.out.println("ioexception " + e);
        }
    }
}
