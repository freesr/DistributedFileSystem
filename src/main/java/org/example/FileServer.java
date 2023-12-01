package org.example;
import com.google.gson.Gson;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.KeyValueClient;
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
import java.util.stream.Collectors;


public class FileServer {

    private static final int SERVER_PORT = 8080; // Example port
    private static final NavigableMap<Integer, ServiceHealth> hashRing = new TreeMap<>();

    private static volatile boolean running = true;
    static String serverId;
    static String serviceName = "file-server3";
    static int numberOfReplicas = 3;
    private static Map<String, FileMetadata> fileMetadataMap = new HashMap<>();


    static class FileMetadata {
        String fileName;
        long fileSize;
        Date creationDate;
        String serverId;
        Set<String> replicatedNodes;

        FileMetadata(String fileName, long fileSize, Date creationDate,String serverId) {
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.creationDate = creationDate;
            this.serverId = serverId;
            this.replicatedNodes = new HashSet<>();
        }

        void addReplicatedNode(String nodeId) {
            replicatedNodes.add(nodeId);
        }

        String toJson() {
            Gson gson = new Gson();
            return gson.toJson(this);
        }

        // Static method to convert JSON back to FileMetadata
        static FileMetadata fromJson(String json) {
            Gson gson = new Gson();
            return gson.fromJson(json, FileMetadata.class);
        }

        // Additional methods as needed...
    }

    public static void main(String[] args) {
        // Initialize the server (e.g., start listening on a socket)

        // Register with Consul
        serverId = "file-server-" + args[0]; // Passed as a command line argument
        int port = Integer.parseInt(args[1]);       // Port number as a command line argument

        // Register with Consul
        registerServiceWithConsul(serverId, serviceName , port);
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
    private static void updateFileMetadataInConsul(String fileName, FileMetadata metadata) {
        Consul consul = Consul.builder().build();
        KeyValueClient kvClient = consul.keyValueClient();

        String metadataJson = metadata.toJson(); // Convert to JSON
        kvClient.putValue( fileName, metadataJson);
    }

    private static FileMetadata getFileMetadataFromConsul(String fileName) {
        Consul consul = Consul.builder().build();
        KeyValueClient kvClient = consul.keyValueClient();

        Optional<String> metadataJson = kvClient.getValueAsString( fileName);
        return metadataJson.map(FileMetadata::fromJson).orElse(null);
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
        String SAVE_DIRECTORY = "Files/"+serverId;
        File serverDirectory = new File(SAVE_DIRECTORY);
        if(!serverDirectory.exists()){
            boolean wasSuccessful = serverDirectory.mkdirs();
            if (!wasSuccessful) {
                System.out.println("Failed to create directory: " + serverDirectory);
            }
        }

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
                case "REPLICATE":
                    handleFileReplicate(dataInputStream, SAVE_DIRECTORY);
                    dataOutputStream.writeUTF("File Replication successfully.");
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

    private static void handleFileReplicate(DataInputStream dataInputStream, String saveDirectory) throws IOException{
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
//        FileMetadata metadata = new FileMetadata(fileName, file.length(), new Date());
//        updateFileMetadataInConsul(fileName, metadata);
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
        FileMetadata metadata = new FileMetadata(fileName, file.length(), new Date(),serverId);
        updateFileMetadataInConsul(fileName, metadata);

        replicateFileToNodes(file,fileName, metadata);
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
        FileMetadata metadata = new FileMetadata(fileName, file.length(), new Date(),serverId);
        updateFileMetadataInConsul(fileName, metadata);
        replicateFileToNodes(file,fileName, metadata);
    }

    private static void replicateFileToNodes(File file,String fileName, FileMetadata metadata) {
        List<ServiceHealth> replicationNodes = selectReplicationNodes();
        for (ServiceHealth node : replicationNodes) {
            boolean success = sendFileToNode(node, fileName, file);
            if (success) {
                metadata.addReplicatedNode(node.getService().getId());
                // Update metadata in Consul again after successful replication
                updateFileMetadataInConsul(file.getName(), metadata);            }
        }
    }


    private static List<ServiceHealth> selectReplicationNodes() {

            Consul consul = Consul.builder().build();
            HealthClient healthClient = consul.healthClient();

            // Fetch all healthy service instances
            List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances(serviceName).getResponse();

            // Shuffle and select a subset for replication
        nodes = nodes.stream()
                .filter(node -> !node.getService().getId().equals(serverId))
                .collect(Collectors.toList());

        // Shuffle and select a subset for replication
        Collections.shuffle(nodes);
        return nodes.stream().limit(Math.min(numberOfReplicas, nodes.size())).collect(Collectors.toList());
        }



    private static boolean sendFileToNode(ServiceHealth node,String fileName, File file) {
        if (!file.exists()) {
            System.out.println("File does not exist: " );
            return false;
        }


        try (Socket socket = new Socket(node.getNode().getAddress(),node.getService().getPort() );
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
             BufferedInputStream fileInputStream = new BufferedInputStream(new FileInputStream(file))) {

            // Send the command and the filename
            dataOutputStream.writeUTF("REPLICATE");
            dataOutputStream.writeUTF(fileName);

            // Send the file data
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                dataOutputStream.write(buffer, 0, bytesRead);
            }

            System.out.println("File has sent replication  " + fileName);
        } catch (IOException e) {
            System.out.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }
        return true; // Placeholder for actual implementation
    }



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
