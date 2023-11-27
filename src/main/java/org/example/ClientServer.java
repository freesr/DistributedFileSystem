package org.example;

import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.health.ServiceHealth;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

public class ClientServer {
    private static final NavigableMap<Integer, ServiceHealth> hashRing = new TreeMap<>();

    public static void main(String[] args) {
        // Client identifier (can be IP, username, etc.)
        String clientId = UUID.randomUUID().toString(); // Example: Use UUID for simplicity

        // Connect to Consul and build the hash ring
        buildHashRing();

        // Select a file server based on consistent hashing
        ServiceHealth selectedServer = selectServer(clientId);

        if (selectedServer == null) {
            System.out.println("No file servers available.");
            return;
        }

        // Connect to the selected file server
        String address = selectedServer.getNode().getAddress();
        int port = selectedServer.getService().getPort();
        System.out.println("Client " + clientId + " connecting to server: " + address + ":" + port);
        String FilePath = "test.txt";
        sendFileToServer(address,port,FilePath);
        // Here, establish a connection to the server (e.g., via sockets)
    }

    private static void buildHashRing() {
        Consul consul = Consul.builder().build();
        HealthClient healthClient = consul.healthClient();
        List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances("file-server").getResponse();

        for (ServiceHealth node : nodes) {
            int hash = hash(node.getService().getId());

            hashRing.put(hash, node);
        }
    }

    private static ServiceHealth selectServer(String clientId) {
        if (hashRing.isEmpty()) {
            return null;
        }
        int clientHash = hash(clientId);
        Integer target = hashRing.ceilingKey(clientHash);
        if (target == null) {
            // Wrap around the hash ring
            target = hashRing.firstKey();
        }
        return hashRing.get(target);
    }

    private static int hash(String key) {
        // Simple hashing function (you may use more sophisticated ones)
        return key.hashCode();
    }

    private static void sendFileToServer(String SERVER_ADDRESS,int SERVER_PORT,String filePath){
        File file = new File(filePath);
        if (!file.exists()) {
            System.out.println("File does not exist: " + filePath);
            return;
        }

        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
             BufferedInputStream fileInputStream = new BufferedInputStream(new FileInputStream(file))) {

            // Send the filename
            dataOutputStream.writeUTF(file.getName());

            // Send the file data
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                dataOutputStream.write(buffer, 0, bytesRead);
            }

            System.out.println("File and filename have been sent successfully: " + filePath);
        } catch (IOException e) {
            System.out.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
