package org.example;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


public class FileServer {

    private static final int SERVER_PORT = 8080; // Example port
    private static final NavigableMap<Integer, ServiceHealth> hashRing = new TreeMap<>();

    private static volatile boolean running = true;
    static String serverId;

    public static void main(String[] args) {
        // Initialize the server (e.g., start listening on a socket)

        // Register with Consul
        serverId = "file-server-" + args[0]; // Passed as a command line argument
        int port = Integer.parseInt(args[1]);       // Port number as a command line argument

        // Register with Consul
        registerServiceWithConsul(serverId, "file-server3", port);
        try {
            startServer(port);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            deregisterServiceFromConsul();
        }
       // startHealthCheckEndpoint(port);

        System.out.println("Server stopped.");



          buildHashRing();



    }

    private static void startHealthCheckEndpoint(int port) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port+1000), 0);
            server.createContext("/health", new HealthCheckHandler());
            server.setExecutor(null); // creates a default executor
            server.start();
            System.out.println("Health check endpoint running on port " + (8081));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class HealthCheckHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response = "OK";
            exchange.sendResponseHeaders(200, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    private static void startServer(int port) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port+1000), 0);
        httpServer.createContext("/health", new HealthCheckHandler()); // Health check endpoint
        httpServer.setExecutor(null); // Default executor
        httpServer.start();
        System.out.println("HTTP Server started on port: " + port);

        // Start the Socket Server for handling client connections
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Socket Server started, listening on: " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getInetAddress());

                // Handle each client connection in a separate thread
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("Socket Server exception: " + e.getMessage());
            e.printStackTrace();
        }

    }



    private static void buildHashRing() {
        // Build the hash ring logic as in the client
        Consul consul = Consul.builder().build();
        HealthClient healthClient = consul.healthClient();
        List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances("file-server3").getResponse();

        for (ServiceHealth node : nodes) {
            int hash = node.getService().getId().hashCode();
            hashRing.put(hash, node);
        }
    }

    private static void handleClient(Socket clientSocket) {
        String SAVE_DIRECTORY = "Files"; // Directory to save received files

        try (DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream())) {

            String command = dataInputStream.readUTF(); // Read the command (CREATE or UPLOAD)

            switch (command) {
                case "CREATE":
                    handleFileCreation(dataInputStream, SAVE_DIRECTORY);
                    dataOutputStream.writeUTF("File created successfully.");
                    break;
                case "UPLOAD":
                    handleFileUpload(dataInputStream, SAVE_DIRECTORY);
                    dataOutputStream.writeUTF("File uploaded successfully.");
                    break;
                default:
                    dataOutputStream.writeUTF("Unknown command.");
            }
        } catch (IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    private static void handleFileUpload(DataInputStream dataInputStream, String saveDirectory) throws IOException {
        String fileName = dataInputStream.readUTF();
        File directory = new File(saveDirectory);
        if (!directory.exists()) {
            directory.mkdir();
        }
        File file = new File(directory, fileName);

        try (BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(file))) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = dataInputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
        }

        // replicateFileToNearestNodes(file); // Uncomment if replication logic is to be applied
    }

    private static void handleFileCreation(DataInputStream dataInputStream, String saveDirectory) throws IOException {
        String fileName = dataInputStream.readUTF();
        String fileContent = dataInputStream.readUTF();

        File directory = new File(saveDirectory);
        if (!directory.exists()) {
            directory.mkdir();
        }

        File file = new File(directory, fileName);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(fileContent);
        }
    }

    private static void replicateDataToNearestNodes(String data) {
        // Find the two nearest nodes
        List<ServiceHealth> nearestNodes = findNearestNodes();

        // Replicate data to these nodes
        for (ServiceHealth node : nearestNodes) {
            // Logic to replicate data to the node
            // This could be an HTTP request, a socket connection, etc.
        }
    }


    private static void replicateFileToNearestNodes(File file) {
        List<ServiceHealth> nearestNodes = findNearestNodes();
        for (ServiceHealth node : nearestNodes) {
            // Replicate the file to each nearest node
            //sendFileToNode(node, file);
        }
    }

//    private static void sendFileToNode(ServiceHealth node, File file) {
//        try {
//            URL url = new URL("http://" + node.getService().getAddress() + ":" + node.getService().getPort() + "/replicate");
//            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//            conn.setDoOutput(true);
//            conn.setRequestMethod("POST");
//            conn.setRequestProperty("Content-Type", "application/octet-stream");
//
//            try (OutputStream os = conn.getOutputStream(); FileInputStream fis = new FileInputStream(file)) {
//                byte[] buffer = new byte[4096];
//                int bytesRead;
//                while ((bytesRead = fis.read(buffer)) != -1) {
//                    os.write(buffer, 0, bytesRead);
//                }
//            }
//
//            int responseCode = conn.getResponseCode();
//            if (responseCode == HttpURLConnection.HTTP_OK) {
//                System.out.println("File replicated to node: " + node.getService().getId());
//            } else {
//                System.out.println("Failed to replicate file to node: " + node.getService().getId());
//            }
//        } catch (IOException e) {
//            System.out.println("Error replicating file to node: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }


    private static List<ServiceHealth> findNearestNodes() {
        List<ServiceHealth> nearestNodes = new ArrayList<>();
        int currentServerHash =  hash(serverId);/* Your current server's hash value */;
        boolean firstNodeFound = false;

        // Iterate over the hash ring
        for (Map.Entry<Integer, ServiceHealth> entry : hashRing.tailMap(currentServerHash, true).entrySet()) {
            if (!firstNodeFound) {
                firstNodeFound = true; // Skip the current server itself
                continue;
            }

            nearestNodes.add(entry.getValue());
            if (nearestNodes.size() == 2) {
                break;
            }
        }

        // If the end of the ring bn  is reached and less than two nodes are found, start from the beginning
        if (nearestNodes.size() < 2) {
            for (Map.Entry<Integer, ServiceHealth> entry : hashRing.entrySet()) {
                if (nearestNodes.contains(entry.getValue())) {
                    continue; // Avoid duplicates
                }

                nearestNodes.add(entry.getValue());
                if (nearestNodes.size() == 2) {
                    break;
                }
            }
        }

        return nearestNodes;        // Logic to find two nearest nodes on the hash ring
        // This depends on the server's own position on the ring and the positions of other servers

    }

    private static void registerServiceWithConsul(String serviceId, String serviceName, int port) {
        Consul consul = Consul.builder().build();
        AgentClient agentClient = consul.agentClient();

        Registration.RegCheck regCheck = Registration.RegCheck.http(
                "http://localhost:" + (port+1000) + "/health", 10L); // Health check every 10 seconds

        ImmutableRegistration registration = ImmutableRegistration.builder()
                .id(serviceId)
                .name(serviceName)
                .addChecks(regCheck)
                .port(port)
                .build();

        agentClient.register(registration);
        System.out.println("Registered service with Consul: " + serviceId);
    }
    private static void deregisterServiceFromConsul() {
        Consul consul = Consul.builder().build();
        AgentClient agentClient = consul.agentClient();
        agentClient.deregister(serverId);
        System.out.println("Deregistered service from Consul: " + serverId);
    }

    private static int hash(String key) {
        // Simple hashing function (you may use more sophisticated ones)
        return key.hashCode();
    }
}
