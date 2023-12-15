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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileServer {

    private static volatile boolean running = true;
    static String serverId;
    static String serviceName = "file-server3";
    static int numberOfReplicas = 2;
    static int leaseDuration = 600000;
    private static final Logger logger = LoggerFactory.getLogger(FileServer.class);

    private static Map<String, FileMetadata> fileMetadataMap = new HashMap<>();
    private  static KeyValueClient kvClient = Consul.builder().build().keyValueClient();
    private static Map<String, Long> temporaryFiles = new HashMap<>();


    private static class ServerInfo  {
        private String serverId;
        private String address;
        private int port;
        private int fileCount;
        private boolean serverInactive;

        private boolean primaryServer;


        ServerInfo(String serverId,String address, int port,int fileCount,boolean serverInactive, boolean primaryServer) {
            this.serverId = serverId;
            this.address = address;
            this.port = port;
            this.fileCount = fileCount;
            this.serverInactive = serverInactive;
            this.primaryServer = primaryServer;
        }
        public String getServerId() {
            return serverId;
        }

        public boolean isPrimaryServer() {
            return primaryServer;
        }

        public void setPrimaryServer(boolean primaryServer) {
            this.primaryServer = primaryServer;
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

        //  convert JSON back to FileMetadata
        static FileMetadata fromJson(String json) {
            Gson gson = new Gson();
            return gson.fromJson(json, FileMetadata.class);
        }

    }

    public static void main(String[] args) {

        serverId = "file-server-" + args[0];
        int port = Integer.parseInt(args[1]);       // Port number

        registerToConsul(serverId, "127.0.0.1", port, false); //server is active


        // Register with Consul
        registerServiceWithConsul(serverId, serviceName , port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            updateServerStatusInConsul(serverId, true); //  server is inactive
        }));
        startCleanupTask();

        try {
            startServer(port);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            deregisterServiceFromConsul();
        }
        logger.info("Server stopped.");
        System.out.println("Server stopped.");

    }

    private static boolean requestLease(String fileName, String serverId) {
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
        httpServer.createContext("/health", new HealthCheckHandler());
        httpServer.setExecutor(null);
        httpServer.start();
        logger.info("HTTP Server started on port: " + port);
        System.out.println("HTTP Server started on port: " + port);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Socket Server started, listening on: " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                logger.info("Client connected: " + clientSocket.getInetAddress());
                System.out.println("Client connected: " + clientSocket.getInetAddress());

                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("Socket Server exception: " + e.getMessage());
            logger.error("Socket Server exception: " + e.getMessage());
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


    private static void handleClient(Socket clientSocket) {
        String SAVE_DIRECTORY = "Files/"+serverId;
        File serverDirectory = new File(SAVE_DIRECTORY);
        if(!serverDirectory.exists()){
            boolean wasSuccessful = serverDirectory.mkdirs();
            if (!wasSuccessful) {
                System.out.println("Failed to create directory: " + serverDirectory);
                logger.info("Failed to create directory: " + serverDirectory);

            }
        }

        try (DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream())) {

            String command = dataInputStream.readUTF(); // Read the command (CREATE or UPLOAD)
            String fileName ;

            switch (command) {
                case "CREATE":
                    handleFileCreation(dataInputStream,dataOutputStream, SAVE_DIRECTORY);
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
                    if (requestLease(fileName, serverId)) {
                        handleReadRequest(dataInputStream, dataOutputStream,fileName,"WRITE");

                    } else {
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
                case "UPDATE_REPLICA":
                    handleUpdateReplica(dataInputStream);
                    dataOutputStream.writeUTF("Replica updated successfully.");
                    break;
                case "DELETE":
                    fileName = dataInputStream.readUTF();
                    handleFileDeletion(dataOutputStream, fileName);
                    break;
                case "DELETE_REPLICA":
                    String replicaFileName = dataInputStream.readUTF();
                    deleteLocalFile(replicaFileName);
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
            logger.error("Error handling client: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing client socket: " + e.getMessage());
                logger.error("Error closing client socket: " + e.getMessage());
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


    private static void handleReadRequest(DataInputStream dataInputStream, DataOutputStream dataOutputStream, String fileName,String mode) throws IOException {
        File localFile = new File("Files/" + serverId, fileName);
        dataOutputStream.writeUTF(mode+" Running");

        // Check if file exists locally
        if (!localFile.exists()) {
            FileMetadata metadata = getFileMetadataFromConsul(fileName);

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

    private static void startCleanupTask() {
        logger.info("starting cleanup task");
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(FileServer::cleanupTemporaryFiles, 1, 1, TimeUnit.HOURS);
    }

    private static void cleanupTemporaryFiles() {
        long oneHourAgo = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        temporaryFiles.entrySet().removeIf(entry -> {
            if (entry.getValue() < oneHourAgo) {
                new File("Files/" + serverId, entry.getKey()).delete();
                return true;
            }
            return false;
        });
    }

    private static void handleUpdateReplica(DataInputStream dataInputStream) throws IOException {
        String fileName = dataInputStream.readUTF();
        int contentLength = dataInputStream.readInt();
        byte[] contentBytes = new byte[contentLength];
        dataInputStream.readFully(contentBytes);

        File file = new File("Files/" + serverId, fileName);
        Files.write(file.toPath(), contentBytes);

        // Optionally, log or print a message confirming the update
        logger.info("Replica updated: " + fileName);
        System.out.println("Replica updated: " + fileName);
    }

    private static void  selectNewPrimaryServerAndRead(String fileName, DataOutputStream dataOutputStream, DataInputStream dataInputStream, String mode,FileMetadata metadata) throws IOException {
        ServerInfo newPrimaryServer = selectServerBasedOnFileCount(metadata);

        if (newPrimaryServer != null) {
            // Update the primary server in metadata and save it in Consul
            metadata.serverId = newPrimaryServer.serverId;
            metadata.replicatedNodes.remove(metadata.serverId);
            List<ServerInfo> replicationTargets = selectServersForReplicationFail(1,metadata);
            File file = new File("Files/" + serverId, fileName);
           // updateFileMetadataInConsul(fileName, metadata);

            // Fetch the file from the new primary server
            fetchFileFromServer(newPrimaryServer.serverId, fileName, new File("Files/" + serverId, fileName), dataOutputStream, dataInputStream, mode);
            replicateFileToNodes(file, fileName, metadata, replicationTargets);

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
            if(mode.equals("OPEN")) {
                dataOutputStream.writeUTF("FILE OPENED");
                while (true) {
                    String command = dataInputStream.readUTF();
                    if (command.equals("CLOSE")) {
                        break;
                    } else if (command.equals("SEEK")) {
                        int seekPos = dataInputStream.readInt();
                        try (RandomAccessFile randomAccessFile = new RandomAccessFile(localFile, "r")) {
                            if (seekPos > randomAccessFile.length()) {
                                dataOutputStream.writeUTF("Seek position is beyond the file length.");
                            } else {
                                randomAccessFile.seek(seekPos);
                                byte[] buffer = new byte[1024]; // Read up to 1024 bytes
                                int bytesRead = randomAccessFile.read(buffer);
                                String content = new String(buffer, 0, bytesRead);
                                dataOutputStream.writeUTF(content);
                            }
                        } catch (FileNotFoundException e) {
                            logger.error("File not found: " + fileName);
                            dataOutputStream.writeUTF("File not found: " + fileName);
                        } catch (IOException e) {
                            logger.error("IO Error: " + e.getMessage());
                            dataOutputStream.writeUTF("IO Error: " + e.getMessage());
                        }
                    }
                }
            } else if (mode.equals("READ")) {
                byte[] fileContent = Files.readAllBytes(localFile.toPath());
                dataOutputStream.writeInt(fileContent.length);
                dataOutputStream.write(fileContent);
            } else if (mode.equals("WRITE")) {
                byte[] fileContent = Files.readAllBytes(localFile.toPath());
                dataOutputStream.writeInt(fileContent.length);
                dataOutputStream.write(fileContent);
                String command_new = dataInputStream.readUTF();
                if ("EDITED_CONTENT".equals(command_new)) {
                    updateFileContent(dataInputStream, fileName);
                }
            }
        } else {
            dataOutputStream.writeInt(0); // Indicate file not found
        }

    }

    private static void updateFileContent(DataInputStream dataInputStream, String fileName) throws IOException {
        int contentLength = dataInputStream.readInt();
        byte[] contentBytes = new byte[contentLength];
        dataInputStream.readFully(contentBytes);

        // Fetch metadata to determine the primary server
        FileMetadata metadata = getFileMetadataFromConsul(fileName);
        if (metadata == null) {
            logger.error("File metadata not found for: " + fileName);
            return;
        }

        // Update the local file if this server is the primary server
        if (metadata.serverId.equals(serverId)  || metadata.replicatedNodes.contains(serverId) ) {
            File localFile = new File("Files/" + serverId, fileName);
            Files.write(localFile.toPath(), contentBytes);
        } else {
            // Send the updated content to the primary server
            sendUpdatedFileToReplica(metadata.serverId, fileName, contentBytes);
        }

        // Release the lease after updating the primary server
        releaseLease(fileName);

        // Update the replicas
        for (String replicatedNodeId : metadata.replicatedNodes) {
            if (!replicatedNodeId.equals(serverId)) { // Don't send to self
                sendUpdatedFileToReplica(replicatedNodeId, fileName, contentBytes);
            }
        }

            logger.info("File updated successfully: " + fileName);
            System.out.println("File updated successfully: " + fileName);
    }

    private static void sendUpdatedFileToReplica(String nodeId, String fileName, byte[] fileContent) {
        ServerInfo serverInfo = getServerDetailsFromConsul(nodeId);
        if (serverInfo == null) {
            logger.error("Replica server info not found for node ID: " + nodeId);
            return;
        }

        try (Socket socket = new Socket(serverInfo.getAddress(), serverInfo.getPort());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            out.writeUTF("UPDATE_REPLICA");
            out.writeUTF(fileName);
            out.writeInt(fileContent.length);
            out.write(fileContent);
        } catch (IOException e) {
            logger.error("Error occurred while updating replica on node " + nodeId + ": " + e.getMessage());
        }
    }
    private static void deleteLocalFile(String fileName) {
        File file = new File("Files/" + serverId, fileName);
        if (file.exists()) {
            if (!file.delete()) {
                logger.info("Failed to delete replica: " + fileName);
                System.out.println("Failed to delete replica: " + fileName);
            }
        }
    }

    private static void fetchFileFromServer(String serverId, String fileName, File localFile,DataOutputStream dataOutputStream,DataInputStream dataInputStream,String mode) throws IOException {
        ServerInfo serverInfo = getServerDetailsFromConsul(serverId);
        if (serverInfo == null) {
            logger.error("Server details not found for server ID: " + serverId);
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
                temporaryFiles.put(fileName, System.currentTimeMillis());
                sendReadToServer(fileName,dataOutputStream,dataInputStream,mode);
            } else {
                logger.error("File not found on remote server");
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
        return null;
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
        // Check if the file exists locally
        File file = new File("Files/" + serverId, fileName);
        boolean localFileExists = file.exists();

        // Fetch metadata from Consul
        FileMetadata metadata = getFileMetadataFromConsul(fileName);
        if (metadata != null) {
            // Delete the file if it exists locally
            boolean deleteSuccess = localFileExists && file.delete();

            // Request deletion from the primary server and replicas
            metadata.replicatedNodes.forEach(nodeId -> deleteReplicaOnNode(nodeId, fileName));
            if (!serverId.equals(metadata.serverId)) { // avoid double deletion if current server is the primary
                deleteReplicaOnNode(metadata.serverId, fileName);
            }

            // Delete metadata from Consul
            kvClient.deleteKey("files/" + fileName);

            if (deleteSuccess || !localFileExists) {
                dataOutputStream.writeUTF("File and its replicas deleted successfully.");
            } else {
                dataOutputStream.writeUTF("Failed to delete the file.");
            }

            // Update file count in Consul if the file existed locally
            if (localFileExists) {
                updateFileCountInConsul(serverId, false);
            }
        } else {
            // Handle case where file metadata is not found in Consul
            if (localFileExists) {
                // Delete the file locally if it exists without metadata
                boolean deleteSuccess = file.delete();
                if (deleteSuccess) {
                    dataOutputStream.writeUTF("Local file without metadata deleted successfully.");
                } else {
                    dataOutputStream.writeUTF("Failed to delete the local file without metadata.");
                }
                updateFileCountInConsul(serverId, false);
            } else {
                // File not found anywhere
                dataOutputStream.writeUTF("File not found.");
            }
        }
    }


    private static void deleteReplicaOnNode(String nodeId, String fileName) {
        ServerInfo serverInfo = getServerDetailsFromConsul(nodeId);

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



    private static void handleFileCreation(DataInputStream dataInputStream,DataOutputStream dataOutputStream, String saveDirectory) throws IOException {
        String fileName = dataInputStream.readUTF();
        String fileContent = dataInputStream.readUTF();

        File directory = new File(saveDirectory);
        if (!directory.exists()) {
            directory.mkdir();
        }

        File file = new File(directory, fileName);
        FileMetadata existingMetadata = getFileMetadataFromConsul(fileName);
        if (existingMetadata != null) {
            dataOutputStream.writeUTF("Filename exists");
        }else{
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write(fileContent);
            }

            FileMetadata metadata = new FileMetadata(fileName, file.length(), new Date(),serverId);
            updateFileMetadataInConsul(fileName, metadata);

            updateFileCountInConsul(serverId, true);

            List<ServerInfo> replicationTargets = selectServersForReplication(numberOfReplicas);
            replicateFileToNodes(file, fileName, metadata, replicationTargets);
        }

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


    private static void registerToConsul(String serverId, String address, int port, boolean serverInactive) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        String key = "server-info/" + serverId;

        List<String> serverKeys  = kvClient.getKeys("server-info/");
        List<ServerInfo> activeServers = new ArrayList<>();

        // Fetch details for each server and populate the list of active servers
        for (String eachServerKey : serverKeys) {
            kvClient.getValueAsString(eachServerKey).ifPresent(json -> {
                ServerInfo info = ServerInfo.fromJson(json);
                if (!info.isServerInactive()) { // Adding only active servers
                    activeServers.add(info);
                }
            });
        }
        boolean isFirstServer = activeServers.isEmpty();

        ServerInfo info = new ServerInfo(serverId, address, port, 0, false, isFirstServer);

        kvClient.putValue(key, info.toJson());
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
        List<String> serverIds = kvClient.getKeys("server-info/");
        Map<String, Integer> fileCountMap = new HashMap<>();

        // Fetch file count for each active server and populate the map
        for (String eachServerId : serverIds) {
            if (!eachServerId.equals("server-info/"+serverId)) { // Exclude the current server
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
                .map(entry -> getServerDetailsFromConsul(entry.getKey()))
                .collect(Collectors.toList());
    }
    private static List<ServerInfo> selectServersForReplicationFail(int numberOfReplicas,FileMetadata metadata) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        List<String> serverIds = kvClient.getKeys("server-info/"); // Get keys for all server info

        Map<String, Integer> fileCountMap = new HashMap<>();

        // Fetch file count for each active server and populate the map
        for (String eachServerId : serverIds) {
            if (!((metadata.replicatedNodes).contains(eachServerId.split("/")[1]) || metadata.serverId.equals(eachServerId.split("/")[1]))) { // Exclude the current server
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

        updateServerStatusInConsul(serverId, true, false);

        // Select a new primary server
        ServerInfo newPrimaryServer = selectNewPrimaryServer();

        if (newPrimaryServer != null) {
            // Update the new primary server's status in Consul
            updateServerStatusInConsul(newPrimaryServer.getServerId(), false, true);
        }
        Consul consul = Consul.builder().build();
        AgentClient agentClient = consul.agentClient();
        agentClient.deregister(serverId);
        System.out.println("Deregistered service from Consul: " + serverId);
    }

    private static void updateServerStatusInConsul(String serverId, boolean serverInactive, boolean primaryServer) {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        String key = "server-info/" + serverId;

        Optional<String> currentValue = kvClient.getValueAsString(key);
        if (currentValue.isPresent()) {
            ServerInfo info = ServerInfo.fromJson(currentValue.get());
            info.setServerInactive(serverInactive);
            info.setPrimaryServer(primaryServer);

            kvClient.putValue(key, info.toJson());
        }
    }

    private static ServerInfo selectNewPrimaryServer() {
        KeyValueClient kvClient = Consul.builder().build().keyValueClient();
        List<String> serverIds = kvClient.getKeys("server-info/"); // Get keys for all server info

        List<ServerInfo> activeServers = new ArrayList<>();

        // Fetch details for each server and populate the list of active servers
        for (String eachServerId : serverIds) {
            kvClient.getValueAsString(eachServerId).ifPresent(json -> {
                ServerInfo info = ServerInfo.fromJson(json);
                if (!info.isServerInactive() && !info.getServerId().equals(serverId)) { // Exclude the current server
                    activeServers.add(info);
                }
            });
        }

        if (!activeServers.isEmpty()) {
            Random rand = new Random();
            return activeServers.get(rand.nextInt(activeServers.size()));
        }
        return null;
    }
}
