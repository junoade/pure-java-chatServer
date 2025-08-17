package org.chatServer.server;

import org.chatServer.broker.BroadCaster;
import org.chatServer.util.StringValidateUtil;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ClientHandler implements Runnable {
    private Socket socket;
    private BroadCaster broadCaster;

    // 쓰기 전용 큐 + writer 스레드(단일 writer 보장)
    private final BlockingQueue<String> outbox = new LinkedBlockingQueue<>();
    private volatile boolean running = true;

    // 닉네임(선택): 최초 입력 없으면 원격 주소
    private volatile String nickname;

    ClientHandler(Socket socket, BroadCaster broadCaster, String nickname) {
        this.socket = socket;
        this.broadCaster = broadCaster;
        setClientNickname(nickname);
    }

    /**
     * 외부(브로드캐스터)가 메시지를 넣는 진입점.
     * @return 큐에 성공적으로 넣었는지
     */
    public boolean enqueue(String msg) {
        return running && outbox.offer(msg);
    }

    public void run() {
        Thread writerThread = null;
        try (
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter writer = new PrintWriter(
                        new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8)),
                        true)
        ) {
            // Write 전용 쓰레드를 분기한다
            // enqueue를 통해 메시지 outbox 에 들어온 메시지 내용을 전달한다
            writerThread = new Thread(() -> {
                try {
                    while (running) {
                        String msg = outbox.take();
                        writer.println(msg);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    // writer가 끝날 때 소켓 종료 유도
                    try { socket.shutdownOutput(); } catch (IOException ignored) {}
                }
            }, "writer-" + socket.getPort());

            writerThread.start();

            // 환영/도움말
            writer.println("Welcome! /nick <name>, /quit");
            broadCaster.broadcast("** " + nickname + " joined (" + broadCaster.getSize() + " online) **");

            // 읽기 루프
            String line;
            while(running && (line = reader.readLine()) != null) {
                if (line.isBlank()) continue;

                if(StringValidateUtil.validateQuitCommand(line)) {
                    writer.println("Disconnected!");
                    break;
                } else {
                    // 일반 매시지 브로드캐스팅
                    String payload = "[" + nickname + "] " + line;
                    broadCaster.broadcast(payload);
                }


            }

        } catch ( IOException e ) {
            System.err.println("[CLIENT] I/O error (" + nickname + "): " + e.getMessage());
        } finally {
            closeQuietly();
            if (writerThread != null) writerThread.interrupt();
            broadCaster.unregister(this);
            broadCaster.broadcast("** " + nickname + " left **");
        }
    }

    private void setClientNickname(String nickname) {
        if(StringValidateUtil.validateClientNickname(nickname)) {
            this.nickname = nickname;
        } else {
            this.nickname = socket.getRemoteSocketAddress().toString();
        }
    }

    public void closeQuietly() {
        running = false;
        try { socket.close(); } catch (IOException ignored) {}
    }

    public String getRemote() {
        return socket.getRemoteSocketAddress().toString();
    }

}
