    package org.example;


    import com.orbitz.consul.Consul;
    import com.orbitz.consul.HealthClient;
    import com.orbitz.consul.cache.ServiceHealthCache;
    import com.orbitz.consul.model.health.ServiceHealth;

    import java.io.*;
    import java.net.HttpURLConnection;
    import java.net.Socket;
    import java.net.URL;
    import java.net.UnknownHostException;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.util.*;
    import java.util.concurrent.atomic.AtomicReference;
    import java.util.concurrent.Executors;
    import java.util.concurrent.ScheduledExecutorService;
    import java.util.concurrent.TimeUnit;



    public class ClientServer {
        private static final ConsistentHashing hashRing = new ConsistentHashing(3, new ArrayList<>()); // 3 replicas, start with an empty list of servers
        private static final AtomicReference<ServerDetails> connectedServerDetails = new AtomicReference<>();
        private static final String SERVICE_NAME = "file-server3"; // replace with your service name
        private final HealthClient healthClient =  Consul.builder().build().healthClient();
        private static HashMap<String,ServiceHealth> nodeIdPair = new HashMap<String,ServiceHealth>();
        static String userOperation;
        private static  long timetaken;

        private static class ServerDetails {
            String address;
            int port;

            ServerDetails(String address, int port) {
                this.address = address;
                this.port = port;
            }
        }

        public static void main(String[] args) {
            //String clientId = UUID.randomUUID().toString();
            String clientId = "clinet" + Math.random() ;


            buildHashRing();
           // setupServiceWatcher();

            ServiceHealth selectedServer = selectServer(clientId);
            if (selectedServer == null) {
                System.out.println("No file servers available.");
                return;
            }

            String address = selectedServer.getNode().getAddress();
            int port = selectedServer.getService().getPort();
            connectedServerDetails.set(new ServerDetails(address, port));
            startRegularHealthChecks();
            System.out.println("Client " + clientId + " connecting to server: " + address + ":" + port);
            Scanner in = new Scanner(System.in);
            try {
                while (true) {
                    System.out.println("____________________\n");
                    System.out.println("Choose the operation \n 1.Upload Existing file \n 2.Create new file \n 3.Read file \n 4.Write to file \n 5.Delete File \n 6.File Open,Seek and Close \n 0.Exit");
                    userOperation = in.nextLine();
                    if (userOperation.equals("1")) {
                        System.out.println("Enter File Name");
                        String filename = in.nextLine();
                        sendFileToServer(address, port, filename);
                    } else if (userOperation.equals("2")) {
                        System.out.println("Enter the name for the new file:");
                        String fileName = in.nextLine();

                        System.out.println("Enter the content for the new file (end input with a single line containing 'END'):");
                        StringBuilder fileContentBuilder = new StringBuilder();
                        String line;
                        while (!(line = in.nextLine()).equals("END")) {
                            fileContentBuilder.append(line).append("\n");
                        }
                        String fileContent = fileContentBuilder.toString();
                        sendNewFileToServer(address, port, fileName, fileContent);
                    } else if (userOperation.equals("3")) {
                        System.out.println("Enter the name of the file to read:");
                        String fileName = in.nextLine();
                        readFileFromServer(address, port, fileName);
                    } else if (userOperation.equals("4")) {
                        System.out.println("Enter the name of the file to write to:");
                        String fileName = in.nextLine();
                        requestWriteToFile(address, port, fileName);
                    } else if (userOperation.equals("5")) {
                        System.out.println("Enter the name of the file to delete:");
                        String fileName = in.nextLine();
                        deleteFileOnServer(address, port, fileName);
                    } else if (userOperation.equals("6")) {
                        System.out.println("Enter the name of the file to open:");
                        String fileName = in.nextLine();
                        requestServerToOpenFile(address, port, fileName);

                    } else if (userOperation.equals("0")) {
                        System.out.println("Closing application");
                        System.exit(0);
                    } else{
                        System.out.println("Invalid Input. Try Again");
                    }
                }
            }catch (Exception e){

            }
            in.close();
        }

            private static void requestServerToOpenFile(String serverAddress, int serverPort, String fileName) {
                try (Socket socket = new Socket(serverAddress, serverPort);
                     DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                     DataInputStream in = new DataInputStream(socket.getInputStream());
                     Scanner sc = new Scanner(System.in)) {

                    out.writeUTF("OPEN");
                    out.writeUTF(fileName);

                    String response = in.readUTF();
                    if (!"FILE OPENED".equals(response)) {
                        System.out.println("Server response: " + response);
                       // return; // Exit if file not opened
                    }

                    boolean isFileOpen = true;
                    while (isFileOpen) {
                        System.out.println("Enter the seek position (Integer) or type CLOSE to close the file:");
                        String input = sc.nextLine().trim();
                        if ("CLOSE".equals(input)) {
                            out.writeUTF("CLOSE");
                            isFileOpen = false;
                        } else {
                            try {
                                //int seekPosition = Integer.parseInt(input);
                                out.writeUTF("SEEK");
                                out.writeInt(Integer.parseInt(input));
                                String content = in.readUTF();
                                System.out.println("Content at position " + input + ": " + content);
                            } catch (NumberFormatException e) {
                                System.out.println("Invalid input. Please enter a valid integer.");
                            }
                        }
                    }
                    System.out.println("File Closed");
                } catch (IOException e) {
                    System.out.println("Error occurred: " + e.getMessage());
                    e.printStackTrace();
                }
            }


        private static void readFileFromServer(String serverAddress, int serverPort, String fileName) {
            try (Socket socket = new Socket(serverAddress, serverPort);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 DataInputStream in = new DataInputStream(socket.getInputStream())) {

                out.writeUTF("READ");
                out.writeUTF(fileName);

                String response = in.readUTF();
                int fileLength = in.readInt();
                if (fileLength > 0) {
                    byte[] fileContent = new byte[fileLength];
                    in.readFully(fileContent);
                    System.out.println("File content:\n" + new String(fileContent));
                } else {
                    System.out.println("File not found or empty.");
                }
            } catch (IOException e) {
                System.out.println("Error occurred: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Method to delete a file on the server
        private static void deleteFileOnServer(String serverAddress, int serverPort, String fileName) {
            try (Socket socket = new Socket(serverAddress, serverPort);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

                out.writeUTF("DELETE");
                out.writeUTF(fileName);

                System.out.println("Request sent to delete file: " + fileName);
            } catch (IOException e) {
                System.out.println("Error occurred: " + e.getMessage());
                e.printStackTrace();
            }
        }


        private static void requestWriteToFile(String serverAddress, int serverPort, String fileName) {
            Socket socket = null;
            DataOutputStream out = null;
            DataInputStream in = null;
            Path tempFilePath = null;
            try {
                socket = new Socket(serverAddress, serverPort);
                out = new DataOutputStream(socket.getOutputStream());
                in = new DataInputStream(socket.getInputStream());

                out.writeUTF("WRITE");
                out.writeUTF(fileName);

                String response = in.readUTF();
                System.out.println(response);
                if ("Lease not acquired. File is currently locked.".equals(response)) {
                    System.out.println("File is currently not available for writing.");
                    return;
                }

                int fileSize = in.readInt();
                if (fileSize <= 0) {
                    System.out.println("Received an empty file or file not found.");
                    return;
                }

                byte[] fileContent = new byte[fileSize];
                in.readFully(fileContent);

                tempFilePath = Files.createTempFile("editfile_", ".tmp");
                Files.write(tempFilePath, fileContent);

                editFile(tempFilePath.toString());

                byte[] editedContent = Files.readAllBytes(tempFilePath);
                sendEditedContentToServer(out, fileName, new String(editedContent));
            } catch (IOException | InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    if (tempFilePath != null) Files.delete(tempFilePath);
                    if (in != null) in.close();
                    if (out != null) out.close();
                    if (socket != null && !socket.isClosed()) socket.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }

        private static void editFile(String filePath) throws IOException, InterruptedException {
            // Replace "nano" with the command for your preferred editor
            ProcessBuilder processBuilder = new ProcessBuilder("notepad", filePath);
            Process process = processBuilder.inheritIO().start();
            process.waitFor();
        }

        private static void sendEditedContentToServer(DataOutputStream out, String fileName, String editedContent) throws IOException {
            out.writeUTF("EDITED_CONTENT");
            //out.writeUTF(fileName);
            byte[] contentBytes = editedContent.getBytes();
            out.writeInt(contentBytes.length);
            out.write(contentBytes);
            System.out.println("Edited content sent to server.");

        }

        private static void startRegularHealthChecks() {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(() -> {
                ServerDetails serverDetails = connectedServerDetails.get();
                if (serverDetails != null) {
                    String healthCheckUrl = "http://" + serverDetails.address + ":" + (serverDetails.port+1000) + "/health";
                    checkServerHealth(healthCheckUrl);
                }
            }, 0, 10, TimeUnit.SECONDS);
        }


        private static void checkServerHealth(String urlString) {
            try {
                URL url = new URL(urlString);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.connect();

                int responseCode = conn.getResponseCode();
                if (responseCode != 200) {
                    System.out.println("Server health check failed");
                }
//                else {
//                    System.out.println("Server is healthy");
//                }
            } catch (IOException e) {
                System.out.println("Failed to perform health check: " + e.getMessage());
            }
        }


        private static void buildHashRing() {
            Consul consul = Consul.builder().build();
            HealthClient healthClient = consul.healthClient();
            List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances("file-server3").getResponse();


            for (ServiceHealth node : nodes) {
                String serverId = node.getService().getId();
                nodeIdPair.put(serverId,node);
                hashRing.addServer(serverId); // Use the addServer method of ConsistentHashing
            }
        }

        private static ServiceHealth selectServer(String clientId) {
            if (hashRing.isEmpty()) {
                return null;
            }
            String selectedServerId = hashRing.get(clientId);
            return nodeIdPair.get(selectedServerId);
        }
//
//        private static int hash(String key) {
//            // Simple hashing function (you may use more sophisticated ones)
//            return key.hashCode();
//        }

        private static void sendFileToServer(String SERVER_ADDRESS,int SERVER_PORT,String filePath){
            File file = new File(filePath);
            if (!file.exists()) {
                System.out.println("File does not exist: " + filePath);
                return;
            }

            try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                 DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                 BufferedInputStream fileInputStream = new BufferedInputStream(new FileInputStream(file))) {

                // Send the command and the filename
                dataOutputStream.writeUTF("UPLOAD");
                dataOutputStream.writeUTF(filePath);

                // Send the file size
//                long fileSize = file.length();
//                dataOutputStream.writeLong(fileSize);

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

        private static void sendNewFileToServer(String SERVER_ADDRESS, int SERVER_PORT, String fileName, String fileContent) {
            timetaken = System.currentTimeMillis();
            try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                // Send the command, file name, and file content
                out.writeUTF("CREATE");
                out.writeUTF(fileName);
                out.writeUTF(fileContent);

                // Read server response
                System.out.println("Server says: " + in.readLine());
                timetaken = System.currentTimeMillis()- timetaken;
                System.out.println("Time taken: " + timetaken);

            } catch (IOException e) {
                System.out.println("Error occurred: " + e.getMessage());
                e.printStackTrace();
            }
        }

    }
