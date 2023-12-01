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
    import java.util.*;
    import java.util.concurrent.atomic.AtomicReference;
    import java.util.concurrent.Executors;
    import java.util.concurrent.ScheduledExecutorService;
    import java.util.concurrent.TimeUnit;



    public class ClientServer {
        private static final NavigableMap<Integer, ServiceHealth> hashRing = new TreeMap<>();
        private static final AtomicReference<ServerDetails> connectedServerDetails = new AtomicReference<>();
        private static final String SERVICE_NAME = "file-server3"; // replace with your service name
        private final HealthClient healthClient =  Consul.builder().build().healthClient();
        static String userOperation;

        private static class ServerDetails {
            String address;
            int port;

            ServerDetails(String address, int port) {
                this.address = address;
                this.port = port;
            }
        }

        public static void main(String[] args) {
            String clientId = UUID.randomUUID().toString();

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

            try (Scanner in = new Scanner(System.in)) {
                while (true) {
                    System.out.println("____________________\n");
                    System.out.println("Choose the operation \n 1.Upload Existing file \n 2.Create new file \n 0.Exit");
                    userOperation = in.nextLine();
                    if (userOperation.equals("1")) {
                        System.out.println("Enter File Path");
                        String filePath = in.nextLine();
                        System.out.println("Enter File Name");
                        String filename = in.nextLine();
                        sendFileToServer(address, port, filePath, filename);
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
                    } else if (userOperation.equals("0")) {
                        System.out.println("Closing application");
                        System.exit(0);
                    }
                }
            }
        }

        private static void setupServiceWatcher() {
            Consul consul = Consul.builder().withReadTimeoutMillis(11000).build();
            HealthClient healthClient = consul.healthClient();
            ServiceHealthCache healthCache = ServiceHealthCache.newCache(healthClient, SERVICE_NAME);

            healthCache.addListener(newValues -> {
                System.out.println("Service change detected!");
                // Handle service changes, e.g., rebuilding the hash ring or notifying the user
                buildHashRing();
            });

            try {
                healthCache.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
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

        private static void sendFileToServer(String SERVER_ADDRESS,int SERVER_PORT,String filePath, String fileName){
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
            try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                // Send the command, file name, and file content
                out.writeUTF("CREATE");
                out.writeUTF(fileName);
                out.writeUTF(fileContent);

                // Read server response
                System.out.println("Server says: " + in.readLine());
            } catch (IOException e) {
                System.out.println("Error occurred: " + e.getMessage());
                e.printStackTrace();
            }
        }

    }
