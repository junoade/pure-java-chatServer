package org.chatServer;

import org.chatServer.broker.BroadCasterImpl;
import org.chatServer.server.Server;

public class Main {
    public static void main(String[] args) {

        // final int SERVER_PORT = Integer.parseInt(args[0]);
        final int SERVER_PORT = 9091;
        // Set<ClientHandler> subs = ConcurrentHashMap.newKeySet();
        BroadCasterImpl broker = new BroadCasterImpl();
        Server server = new Server(SERVER_PORT, broker, 16);

        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}