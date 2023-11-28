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

//        while (running) {
//            // Here you'd have code to listen for and handle client connections
//            // For example, accepting connections on a ServerSocket
//
//            try {
//                // This is a placeholder for demonstration
//                Thread.sleep(1000); // Sleep for a second (simulating a server process)
//            } catch (InterruptedException ie) {
//                Thread.currentThread().interrupt();
//                System.out.println("Server interrupted.");
//                break;
//            }
//        }

       // new FileServer().startServer(port);

          buildHashRing();
//        Thread dthread = new Thread (new ListenerPort(port));
//        dthread.setName("AttendFileDownloadRequest");
//        dthread.start();
//        try (ServerSocket serverSocket = new ServerSocket(SERVER_PORT)) {
//            System.out.println("FileServer is running on port " + SERVER_PORT);
//
//            while (true) {
//                Socket clientSocket = serverSocket.accept();
//                handleClient(clientSocket);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }


        // Server's main loop to handle client connections
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

    private static void socketCnnection(int port){
//        try (ServerSocket serverSocket = new ServerSocket(port)) {
//            System.out.println("Server started, listening on: " + port);
//
//            while (true) {
//                Socket clientSocket = serverSocket.accept();
//                System.out.println("Client connected: " + clientSocket.getInetAddress());
//
//                new Thread(() -> handleClient(clientSocket)).start();
//            }
//        } catch (IOException e) {
//            System.out.println("Server exception: " + e.getMessage());
//            e.printStackTrace();
       // }
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
         String SAVE_DIRECTORY = "../../../../Files"; // Directory to save received files

        try (DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream())) {
            // Read the filename
            String fileName = dataInputStream.readUTF();
            File file = new File(SAVE_DIRECTORY + fileName);

            // Read the file data
            try (BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(file))) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = dataInputStream.read(buffer)) != -1) {
                    fileOutputStream.write(buffer, 0, bytesRead);
                }
            }

            System.out.println("File received and saved: " + file.getAbsolutePath());
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

    private static void replicateDataToNearestNodes(String data) {
        // Find the two nearest nodes
        List<ServiceHealth> nearestNodes = findTwoNearestNodes();

        // Replicate data to these nodes
        for (ServiceHealth node : nearestNodes) {
            // Logic to replicate data to the node
            // This could be an HTTP request, a socket connection, etc.
        }
    }

    private static List<ServiceHealth> findTwoNearestNodes() {
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