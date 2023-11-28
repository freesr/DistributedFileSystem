    package org.example;

    import com.orbitz.consul.Consul;
    import com.orbitz.consul.HealthClient;
    import com.orbitz.consul.model.health.ServiceHealth;

    import java.io.*;
    import java.net.HttpURLConnection;
    import java.net.Socket;
    import java.net.URL;
    import java.net.UnknownHostException;
    import java.util.List;
    import java.util.NavigableMap;
    import java.util.TreeMap;
    import java.util.UUID;
    import java.util.concurrent.Executors;
    import java.util.concurrent.ScheduledExecutorService;
    import java.util.concurrent.TimeUnit;

    public class ClientServer {
        private static final NavigableMap<Integer, ServiceHealth> hashRing = new TreeMap<>();

        public static void main(String[] args) {
            // Client identifier (can be IP, username, etc.)
            String clientId = UUID.randomUUID().toString(); // Example: Use UUID for simplicity

            startRegularHealthChecks();

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

        private static void startRegularHealthChecks() {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(() -> {
                // Assuming the health check endpoint is running on port 8081
                checkServerHealth("http://localhost:8081/health");
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
                } else {
                    System.out.println("Server is healthy");
                }
            } catch (IOException e) {
                System.out.println("Failed to perform health check: " + e.getMessage());
            }
        }


        private static void buildHashRing() {
            Consul consul = Consul.builder().build();
            HealthClient healthClient = consul.healthClient();
            List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances("file-server3").getResponse();

            for (ServiceHealth service : nodes) {
                if (service.getChecks().stream().allMatch(check -> check.getStatus().equals("passing"))) {
                    System.out.println("Active Service: " + service.getService().getId());
                    // Connect to the service here
                }
            }
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