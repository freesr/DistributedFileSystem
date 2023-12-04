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
import java.net.*;
import java.nio.file.Files;
import java.util.*;

import java.util.stream.Collectors;


public class FileServer {

    private static final int SERVER_PORT = 8080; // Example port
    private static final NavigableMap<Integer, ServiceHealth> hashRing = new TreeMap<>();

    private static volatile boolean running = true;
    static String serverId;
    static String serviceName = "file-server3";
    static int numberOfReplicas = 3;
    static int leaseDuration = 600000;
    private static Map<String, FileMetadata> fileMetadataMap = new HashMap<>();
    private  static KeyValueClient kvClient = Consul.builder().build().keyValueClient();

    static class FileMetadata {
        String fileName;
        long fileSize;
        Date creationDate;
        String serverId;
        Set<String> replicatedNodes;
        String lesseeServerId;
        boolean isLeased;
        String leasedBy;
        long leaseExpiryTime;




        FileMetadata(String fileName, long fileSize, Date creationDate,String serverId) {
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.creationDate = creationDate;
            this.serverId = serverId;
            this.replicatedNodes = new HashSet<>();
            this.isLeased = false;
            this.leaseExpiryTime =0;
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

        System.out.println("Server stopped.");

          buildHashRing();

    }

    private static boolean tryAcquireLease(String fileName, String serverId) {
        Optional<String> metadataJson = kvClient.getValueAsString("files/" + fileName);
        FileMetadata metadata = metadataJson.map(FileMetadata::fromJson).orElse(null);

        long currentTime = System.currentTimeMillis();
        if (!metadata.isLeased || (metadata.leaseExpiryTime != 0 && currentTime > metadata.leaseExpiryTime)) {
            metadata.isLeased = true;
            metadata.leasedBy = serverId;
            metadata.leaseExpiryTime = currentTime + leaseDuration;

            kvClient.putValue("files/" + fileName, metadata.toJson());
            return true;
        }
        return false;
    }

    private static void releaseLease(String fileName) {
        Optional<String> metadataJson = kvClient.getValueAsString("files/" + fileName);
        FileMetadata metadata = metadataJson.map(FileMetadata::fromJson).orElse(null);

        if (metadata != null) {
            metadata.isLeased = false;
            metadata.leasedBy = null;
            metadata.leaseExpiryTime =0;
            kvClient.putValue("files/" + fileName, metadata.toJson());
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
    private static void updateFileMetadataInConsul(String fileName, FileMetadata metadata) {
        Consul consul = Consul.builder().build();
        KeyValueClient kvClient = consul.keyValueClient();

        String metadataJson = metadata.toJson(); // Convert to JSON
        kvClient.putValue("files/" + fileName, metadataJson);
    }

    private static FileMetadata getFileMetadataFromConsul(String fileName) {
        Consul consul = Consul.builder().build();
        KeyValueClient kvClient = consul.keyValueClient();

        Optional<String> metadataJson = kvClient.getValueAsString("files/"+ fileName);
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
            String fileName ;

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
                case "READ":
                     fileName = dataInputStream.readUTF();
                    handleReadRequest(dataInputStream, dataOutputStream,fileName,"READ");
                    break;
                case "WRITE":
                     fileName = dataInputStream.readUTF();
                    if (tryAcquireLease(fileName, serverId)) {
                        handleReadRequest(dataInputStream, dataOutputStream,fileName,"WRITE");

                    } else {
                        // Lease not acquired, inform the client
                        dataOutputStream.writeUTF("Lease not acquired. File is currently locked.");
                    }
                    //handleWriteRequest(dataInputStream, dataOutputStream);
                    break;
                case "READFROMSERVER":
                    handleReadFromServerRequest(dataInputStream,dataOutputStream);
                    break;
                case "EDITED_CONTENT":
                    handleEditedContent(dataInputStream, dataOutputStream);
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

    private static void handleEditedContent(DataInputStream dataInputStream, DataOutputStream dataOutputStream) throws IOException {
        String fileName = dataInputStream.readUTF();
        int contentLength = dataInputStream.readInt();
        byte[] contentBytes = new byte[contentLength];
        dataInputStream.readFully(contentBytes);
        String editedContent = new String(contentBytes);

        File file = new File("Files/" + serverId, fileName);
        Files.write(file.toPath(), editedContent.getBytes());

        // After updating the file, you might want to release the lease or update the file metadata
        // Release the lease or update file metadata here
        // For example: releaseLease(fileName);

        dataOutputStream.writeUTF("File updated successfully.");
    }



    private static void handleReadFromServerRequest(DataInputStream dataInputStream, DataOutputStream dataOutputStream) throws IOException {
        String fileName = dataInputStream.readUTF();
        File file = new File("Files/" + serverId, fileName);

        if (file.exists()) {
            // Send file content back to the requester
            byte[] fileContent = Files.readAllBytes(file.toPath());
            dataOutputStream.writeInt(fileContent.length);
            dataOutputStream.write(fileContent);
        } else {
            dataOutputStream.writeInt(0); // Indicate file not found
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

    private static void handleWriteOperation(String fileName, String fileContent) {
        FileMetadata metadata = getFileMetadataFromConsul(fileName);
        if (metadata.lesseeServerId.equals(serverId) && metadata.isLeased) {
            // Perform write operation
            // After writing, notify the owner server to commit changes
        }
    }

    private static void handleReadRequest(DataInputStream dataInputStream, DataOutputStream dataOutputStream, String fileName,String mode) throws IOException {
        File localFile = new File("Files/" + serverId, fileName);
        dataOutputStream.writeUTF(mode+" Running");

        // Check if file exists locally
        if (!localFile.exists()) {
            FileMetadata metadata = getFileMetadataFromConsul(fileName);

            // Check if file metadata exists and file is on another server
            if (metadata != null && !metadata.serverId.equals(serverId)) {
                // Fetch file from the server where it's located
                try {
                    fetchFileFromServer(metadata.serverId, fileName, localFile,dataOutputStream,dataInputStream,mode);
                } catch (Exception e) {
                    System.out.println("Failed to fetch file from other server: " + e.getMessage());
                    dataOutputStream.writeInt(0); // Indicate file not found or error
                    return;
                }
            }
        }else{
            sendReadToServer(fileName,dataOutputStream,dataInputStream,mode);
        }

    }

    private static void sendReadToServer(String fileName,DataOutputStream dataOutputStream,DataInputStream dataInputStream, String mode) throws IOException{
        File localFile = new File("Files/" + serverId, fileName);
        if (localFile.exists() ) {
            byte[] fileContent = Files.readAllBytes(localFile.toPath());
            dataOutputStream.writeInt(fileContent.length);
            dataOutputStream.write(fileContent);
            if(mode.equals("WRITE")){
                String command = dataInputStream.readUTF(); // Expecting "EDITED_CONTENT" command
                if ("EDITED_CONTENT".equals(command)) {
                    updateFileContent(dataInputStream, fileName);
                }
            }
        } else {
            dataOutputStream.writeInt(0); // Indicate file not found
        }
    }

    private static void updateFileContent(DataInputStream dataInputStream, String fileName) throws IOException {
        int contentLength = dataInputStream.readInt();
        File localFile = new File("Files/" + serverId, fileName);

        if (contentLength > 0) {
            byte[] contentBytes = new byte[contentLength];
            dataInputStream.readFully(contentBytes);
            Files.write(localFile.toPath() , contentBytes);
            releaseLease(fileName);
            System.out.println("File updated successfully: " + fileName);
        }
    }

    private static void fetchFileFromServer(String serverId, String fileName, File localFile,DataOutputStream dataOutputStream,DataInputStream dataInputStream,String mode) throws IOException {
        // Fetch the server details (IP address and port) from Consul
        ServiceHealth server = getServerDetailsFromConsul(serverId);
        String serverAddress = server.getNode().getAddress();
        int serverPort = server.getService().getPort();

        // Connect to the server and request the file
        try (Socket socket = new Socket(serverAddress, serverPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            out.writeUTF("READFROMSERVER");
            out.writeUTF(fileName);

            int fileLength = in.readInt();
            if (fileLength > 0) {
                byte[] fileContent = new byte[fileLength];
                in.readFully(fileContent);
                Files.write(localFile.toPath(), fileContent);
                sendReadToServer(fileName,dataOutputStream,dataInputStream,mode);
            } else {
                throw new IOException("File not found on remote server");
            }
        }
    }

    private static ServiceHealth getServerDetailsFromConsul(String serverId) {
        Consul consul = Consul.builder().build();
        HealthClient healthClient = consul.healthClient();

        // Query for all healthy instances of the service
        List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances("file-server3").getResponse();

        // Search for the server with the given ID
        Optional<ServiceHealth> matchingServer = nodes.stream()
                .filter(node -> node.getService().getId().equals(serverId))
                .findFirst();

        if (matchingServer.isPresent()) {
            return matchingServer.get();
        } else {
            throw new RuntimeException("Server with ID " + serverId + " not found in Consul.");
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
