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
    static int numberOfReplicas = 2;
    static int leaseDuration = 600000;
    private static Map<String, FileMetadata> fileMetadataMap = new HashMap<>();
    private  static KeyValueClient kvClient = Consul.builder().build().keyValueClient();

    private static class ServerInfo  {
        private String serverId; // New attribute
        private String address;
        private int port;
        private int fileCount;
        private boolean serverInactive; // New attribute

        ServerInfo(String serverId,String address, int port,int fileCount,boolean serverInactive) {
            this.serverId = serverId;
            this.address = address;
            this.port = port;
            this.fileCount = fileCount;
            this.serverInactive = serverInactive;
        }
        public String getServerId() {
            return serverId;
        }

        public void setServerId(String serverId) {
            this.serverId = serverId;
        }

        public void setServerInactive(boolean serverInactive) {
            this.serverInactive = serverInactive;
        }

        public boolean isServerInactive() {
            return serverInactive;
        }

        String toJson() {
            Gson gson = new Gson();
            return gson.toJson(this);
        }

        static ServerInfo fromJson(String json) {
            Gson gson = new Gson();
            return gson.fromJson(json, ServerInfo.class);
        }


        public String getAddress() {
            return address;
        }

        public int getPort() {
            return port;
        }

        public int getFileCount() {
            return fileCount;
        }

        public void setFileCount(int fileCount) {
            this.fileCount = fileCount;
        }
    }


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

    }

    public static void main(String[] args) {
        // Initialize the server (e.g., start listening on a socket)

        // Register with Consul
        serverId = "file-server-" + args[0]; // Passed as a command line argument
        int port = Integer.parseInt(args[1]);       // Port number as a command line argument

        registerServerInConsul(serverId, "127.0.0.1", port, false); // false indicates server is active


        // Register with Consul
        registerServiceWithConsul(serverId, serviceName , port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            updateServerStatusInConsul(serverId, true); // true indicates server is inactive
        }));

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
                case "DELETE":
                    fileName = dataInputStream.readUTF();
                    handleFileDeletion(dataOutputStream, fileName);
                    break;
                case "DELETE_REPLICA":
                    String replicaFileName = dataInputStream.readUTF();
                    deleteLocalFile(replicaFileName); // You can reuse or modify the file deletion logic here
                    break;
                case "OPEN":
                    fileName = dataInputStream.readUTF();
                    handleReadRequest(dataInputStream, dataOutputStream,fileName,"OPEN");
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

    private static void handleOpenRequest(DataInputStream dataInputStream, DataOutputStream dataOutputStream, String fileName) {
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
                    System.out.println("Primary server is down. Selecting a new primary server.");
                    selectNewPrimaryServerAndRead(fileName, dataOutputStream, dataInputStream, mode, metadata);
                }
            }
        }else{
            sendReadToServer(fileName,dataOutputStream,dataInputStream,mode);
        }

    }
    private static void  selectNewPrimaryServerAndRead(String fileName, DataOutputStream dataOutputStream, DataInputStream dataInputStream, String mode,FileMetadata metadata) throws IOException {
        ServerInfo newPrimaryServer = selectServerBasedOnFileCount(metadata);

        if (newPrimaryServer != null) {
            // Update the primary server in metadata and save it in Consul
            metadata.serverId = newPrimaryServer.serverId;
            metadata.replicatedNodes.remove(metadata.serverId);
            List<ServerInfo> replicationTargets = selectServersForReplicationFail(1,metadata.serverId);
            File file = new File("Files/" + serverId, fileName);
            replicateFileToNodes(file, fileName, metadata, replicationTargets);
           // updateFileMetadataInConsul(fileName, metadata);

            // Fetch the file from the new primary server
            fetchFileFromServer(newPrimaryServer.serverId, fileName, new File("Files/" + serverId, fileName), dataOutputStream, dataInputStream, mode);
        } else {
            dataOutputStream.writeUTF("Failed to find a new primary server for the file.");
        }
    }

    private static ServerInfo selectServerBasedOnFileCount(FileMetadata metadata) {
        // Logic to select a server with the least file count
        String selectedServerId = null;
        int minFileCount = Integer.MAX_VALUE;
        ServerInfo selectedNode = null;

        for (String serverId : metadata.replicatedNodes) {
            ServerInfo info = getServerDetailsFromConsul(serverId);
            if (info != null && !info.isServerInactive() && info.getFileCount() < minFileCount) {
                minFileCount = info.getFileCount();
                selectedNode = info;
                selectedServerId = serverId;
            }
        }

        return selectedNode; // Returns the ID of the server with the lowest file count
    }



    private static void sendReadToServer(String fileName,DataOutputStream dataOutputStream,DataInputStream dataInputStream, String mode) throws IOException{
        File localFile = new File("Files/" + serverId, fileName);
        if (localFile.exists() ) {
            if(mode.equals("OPEN")){
                dataOutputStream.writeUTF("FILE OPENED");
                while (true){
                    String newResponse = dataInputStream.readUTF();
                    if(newResponse.equals("CLOSE")){
                        break;
                    }else{
                        int seekPos = dataInputStream.readInt();
                        try (RandomAccessFile file = new RandomAccessFile(localFile, "r")) {
                            // Move the file pointer to the specified position
                            if (seekPos > file.length()) {
                                // Position is beyond the end of the file
                                dataOutputStream.writeUTF("Seek position is beyond the file length.");
                            } else {
                                file.seek(seekPos);
                                dataOutputStream.writeUTF("File seeked to position: " + seekPos);
                            }
                        } catch (FileNotFoundException e) {
                            dataOutputStream.writeUTF("File not found: " + fileName);
                        } catch (IOException e) {
                            dataOutputStream.writeUTF("IO Error: " + e.getMessage());
                        }
                    }
                }



            } else if(mode.equals("READ")){
                byte[] fileContent = Files.readAllBytes(localFile.toPath());
                dataOutputStream.writeInt(fileContent.length);
                dataOutputStream.write(fileContent);
            }else  if(mode.equals("WRITE")){
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
    private static void deleteLocalFile(String fileName) {
        File file = new File("Files/" + serverId, fileName);
        if (file.exists()) {
            if (!file.delete()) {
                System.out.println("Failed to delete replica: " + fileName);
            }
        }
    }

    private static void fetchFileFromServer(String serverId, String fileName, File localFile,DataOutputStream dataOutputStream,DataInputStream dataInputStream,String mode) throws IOException {
        // Fetch the server details (IP address and port) from Consul
        ServerInfo serverInfo = getServerDetailsFromConsul(serverId);
        if (serverInfo == null) {
            throw new IOException("Server details not found for server ID: " + serverId);
        }

        String serverAddress = serverInfo.getAddress();
        int serverPort = serverInfo.getPort();

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

    private static ServerInfo getServerDetailsFromConsul(String serverId) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        String key = "server-info/" + serverId;
        Optional<String> value = kvClient.getValueAsString(key);
        if (value.isPresent()) {
            return ServerInfo.fromJson(value.get());
        }
        return null; // Or handle appropriately
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

        updateFileCountInConsul(serverId, true);

        List<ServerInfo> replicationTargets = selectServersForReplication(numberOfReplicas);
        replicateFileToNodes(file, fileName, metadata, replicationTargets);
    }


    private static void handleFileDeletion(DataOutputStream dataOutputStream, String fileName) throws IOException {
        File file = new File("Files/" + serverId, fileName);
        if (file.exists()) {
            boolean deleteSuccess = file.delete();

            FileMetadata metadata = getFileMetadataFromConsul(fileName);
            if (metadata != null) {
                metadata.replicatedNodes.forEach(nodeId -> deleteReplicaOnNode(nodeId, fileName));
                kvClient.deleteKey("files/" + fileName); // Also delete metadata from Consul
            }

            if (deleteSuccess) {
                dataOutputStream.writeUTF("File and its replicas deleted successfully.");
            } else {
                dataOutputStream.writeUTF("Failed to delete the file.");
            }
            updateFileCountInConsul(serverId, false);
        } else {
            dataOutputStream.writeUTF("File not found.");
        }
    }

    private static void deleteReplicaOnNode(String nodeId, String fileName) {
        ServerInfo serverInfo = getServerDetailsFromConsul(serverId);

        String serverAddress = serverInfo.getAddress();
        int serverPort = serverInfo.getPort();

        try (Socket socket = new Socket(serverAddress, serverPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            out.writeUTF("DELETE_REPLICA");
            out.writeUTF(fileName);
        } catch (IOException e) {
            System.out.println("Error occurred while deleting replica on node " + nodeId + ": " + e.getMessage());
        }
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

        updateFileCountInConsul(serverId, true);

        List<ServerInfo> replicationTargets = selectServersForReplication(numberOfReplicas);
        replicateFileToNodes(file, fileName, metadata, replicationTargets);


    }

    private static void replicateFileToNodes(File file, String fileName, FileMetadata metadata, List<ServerInfo> replicationTargets) {
        for (ServerInfo target : replicationTargets) {
            boolean success = sendFileToNode(target, fileName, file);
            if (success) {
                metadata.addReplicatedNode(target.getServerId()); // Assuming getServerId() exists in ServerInfo
                // Update metadata in Consul again after successful replication
                updateFileMetadataInConsul(file.getName(), metadata);
                updateFileCountInConsul(target.getServerId(),true);
            }
        }
    }


    private static void registerServerInConsul(String serverId, String address, int port, boolean serverInactive) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        String key = "server-info/" + serverId;

        ServerInfo info = new ServerInfo(serverId,address,port,0,serverInactive);

        kvClient.putValue(key, info.toJson()); // Assuming toJson() method in ServerInfo class
    }

    private static void updateServerStatusInConsul(String serverId, boolean serverInactive) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        String key = "server-info/" + serverId;

        Optional<String> currentValue = kvClient.getValueAsString(key);
        if (currentValue.isPresent()) {
            ServerInfo info = ServerInfo.fromJson(currentValue.get());
            info.setServerInactive(serverInactive);

            kvClient.putValue(key, info.toJson());
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

    private static void updateFileCountInConsul(String serverId, boolean increment) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        String key = "server-info/" + serverId;
        Optional<String> currentValue = kvClient.getValueAsString(key);
        if (currentValue.isPresent()) {
            ServerInfo info = ServerInfo.fromJson(currentValue.get());
            int currentCount = info.getFileCount();
            currentCount = increment ? currentCount + 1 : Math.max(currentCount - 1, 0);
            info.setFileCount(currentCount);
            kvClient.putValue(key, info.toJson());
        }
    }


    private static List<ServerInfo> selectServersForReplication(int numberOfReplicas) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        List<String> serverIds = kvClient.getKeys("server-info/"); // Get keys for all server info
        Map<String, Integer> fileCountMap = new HashMap<>();

        // Fetch file count for each active server and populate the map
        for (String eachServerId : serverIds) {
            if (!eachServerId.equals(serverId)) { // Exclude the current server
                kvClient.getValueAsString(eachServerId).ifPresent(json -> {
                    ServerInfo info = ServerInfo.fromJson(json);
                    if (!info.isServerInactive()) { // Check if server is active
                        fileCountMap.put(info.getServerId(), info.getFileCount());
                    }
                });
            }
        }


        // Sort the entries by file count and select the top servers with the least file count
        return fileCountMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .limit(numberOfReplicas)
                .map(entry -> getServerDetailsFromConsul(entry.getKey())) // Convert serverId to ServerInfo
                .collect(Collectors.toList());
    }
    private static List<ServerInfo> selectServersForReplicationFail(int numberOfReplicas,String pServerID) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        List<String> serverIds = kvClient.getKeys("server-info/"); // Get keys for all server info
        Map<String, Integer> fileCountMap = new HashMap<>();

        // Fetch file count for each active server and populate the map
        for (String eachServerId : serverIds) {
            if (!(eachServerId.equals(serverId) || !eachServerId.equals(pServerID))) { // Exclude the current server
                kvClient.getValueAsString(eachServerId).ifPresent(json -> {
                    ServerInfo info = ServerInfo.fromJson(json);
                    if (!info.isServerInactive()) { // Check if server is active
                        fileCountMap.put(info.getServerId(), info.getFileCount());
                    }
                });
            }
        }


        // Sort the entries by file count and select the top servers with the least file count
        return fileCountMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .limit(numberOfReplicas)
                .map(entry -> getServerDetailsFromConsul(entry.getKey())) // Convert serverId to ServerInfo
                .collect(Collectors.toList());
    }



    private static boolean sendFileToNode(ServerInfo node,String fileName, File file) {
        if (!file.exists()) {
            System.out.println("File does not exist: " );
            return false;
        }


        try (Socket socket = new Socket(node.getAddress(),node.getPort() );
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
