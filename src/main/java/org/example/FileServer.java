package org.example;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;



public class FileServer {
    private static volatile boolean running = true;

    public static void main(String[] args) {
        // Initialize the server (e.g., start listening on a socket)

        // Register with Consul
        String serverId = "file-server-" + args[0]; // Passed as a command line argument
        int port = Integer.parseInt(args[1]);       // Port number as a command line argument

        // Register with Consul
        registerServiceWithConsul(serverId, "file-server", port);
        while (running) {
            // Here you'd have code to listen for and handle client connections
            // For example, accepting connections on a ServerSocket

            try {
                // This is a placeholder for demonstration
                Thread.sleep(1000); // Sleep for a second (simulating a server process)
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                System.out.println("Server interrupted.");
                break;
            }
        }

        System.out.println("Server stopped.");
        // Server's main loop to handle client connections
    }

    private static void registerServiceWithConsul(String serviceId, String serviceName, int port) {
        Consul consul = Consul.builder().build(); // Connect to Consul on localhost
        AgentClient agentClient = consul.agentClient();

        ImmutableRegistration registration = ImmutableRegistration.builder()
                .id(serviceId)
                .name(serviceName)
                .port(port)
                .build();

        agentClient.register(registration);
        System.out.println("Registered service with Consul: " + serviceId);
    }
}
