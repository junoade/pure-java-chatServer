package org.chatServer.server;

import org.chatServer.broker.BroadCaster;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

public class Server {

    private final int port;
    private final BroadCaster broadcaster;
    private final ExecutorService pool;
    private volatile boolean running = false;
    private ServerSocket serverSocket;


    public Server(int port, BroadCaster broadcaster, int maxThreads) {
        this.port = port;
        this.broadcaster = broadcaster;
        this.pool = new ThreadPoolExecutor(
                Math.max(4, Math.min(16, maxThreads / 2)), // core
                maxThreads,                                 // max
                60L, //  keepAliveTime
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadFactory() {
                    private final ThreadFactory def = Executors.defaultThreadFactory();
                    @Override public Thread newThread(Runnable r) {
                        Thread t = def.newThread(r);
                        t.setName("client-handler-" + t.getId());
                        t.setDaemon(false);
                        return t;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // 임계 시 호출 스레드가 수행
        );

    }

    public void start() throws IOException {
        running = true;
        serverSocket = new ServerSocket(port);
        System.out.println("[SERVER] Listening on port " + port);

        // 그레이스풀 종료 훅
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { shutdown(); } catch (Exception ignored) {}
        }, "shutdown-hook"));

        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                System.out.println("[SERVER] Client connected: " + socket.getRemoteSocketAddress());

                ClientHandler handler = new ClientHandler(socket, broadcaster, "");
                broadcaster.register(handler);
                pool.execute(handler); // handler가 내부적으로 reader+writer를 운용
            } catch (IOException e) {
                if (running) {
                    System.err.println("[SERVER] accept() error: " + e.getMessage());
                }
            }
        }
    }

    public void shutdown() {
        if (!running) return;
        running = false;
        System.out.println("[SERVER] Shutting down...");

        try { if (serverSocket != null && !serverSocket.isClosed()) serverSocket.close(); }
        catch (IOException ignored) {}

        // 새 작업 중단, 큐 비움
        pool.shutdown();
        try {
            if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        broadcaster.shutdown();
        System.out.println("[SERVER] Bye.");
    }
}
