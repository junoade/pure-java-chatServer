package org.chatServer.broker;

import org.chatServer.server.ClientHandler;

public interface BroadCaster {
    void register(ClientHandler client);
    void unregister(ClientHandler client);
    void broadcast(String message);
    int getSize();
    void shutdown();
}
