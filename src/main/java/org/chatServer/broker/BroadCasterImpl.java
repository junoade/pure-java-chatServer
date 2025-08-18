package org.chatServer.broker;

import org.chatServer.server.ClientHandler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BroadCasterImpl implements BroadCaster {

    private final Set<ClientHandler> clients;
    private volatile boolean accepting = true;

    public BroadCasterImpl() {
        this.clients = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void register(ClientHandler client) {
        if(!accepting) { return; }

        clients.add(client);
        System.out.println("[BROADCASTER] Registered: " + client.getRemote());
    }

    @Override
    public void unregister(ClientHandler client) {
        clients.remove(client);
        System.out.println("[BROADCASTER] Registered: " + client.getRemote());
    }

    @Override
    public void broadcast(String message) {
        for(ClientHandler client : clients) {
            if (!client.enqueue(message)) {
                // 전송 불가/종료 상태인 경우 즉시 탈퇴
                unregister(client);
            }
        }
    }

    @Override
    public int getSize() {
        return clients.size();
    }

    @Override
    public void shutdown() {
        accepting = false;
        for (ClientHandler client : clients) {
            try { client.closeQuietly(); } catch (Exception ignored) {}
        }
        clients.clear();
    }
}
