package org.example;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ListenerPort implements Runnable {
    int listen_port;
    Socket connection;
    //Boolean flag;                            //declarations
    ServerSocket server;
    private static final String SAVE_DIRECTORY = "../../../../Files"; // Directory to save received files


    public ListenerPort(int listen_port) {

        this.listen_port = listen_port;
    }


    public void run() {
        try {
            server = new ServerSocket(listen_port);

            while (true) {                                                                       //Listen for Download request
                connection = server.accept();
                try (DataInputStream dataInputStream = new DataInputStream(connection.getInputStream())) {
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
                        connection.close();
                    } catch (IOException e) {
                        System.out.println("Error closing client socket: " + e.getMessage());
                    }
                }

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
