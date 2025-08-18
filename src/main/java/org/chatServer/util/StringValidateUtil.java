package org.chatServer.util;

public class StringValidateUtil {

    public static boolean validateClientNickname(String nickname) {
        if(nickname == null) {
            return false;
        }

        String trimmedNickname = nickname.trim();
        return !trimmedNickname.isEmpty();
    }

    public static boolean validateQuitCommand(String message) {
        return message.contains("/quit") || message.contains("/q");
    }
}
