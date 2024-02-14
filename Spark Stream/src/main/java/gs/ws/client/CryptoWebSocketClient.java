package gs.ws.client;

import org.apache.spark.streaming.receiver.Receiver;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;


import java.net.URI;

public class CryptoWebSocketClient extends WebSocketClient{
        private final Receiver<String> receiver;
    public CryptoWebSocketClient(String serverUri,Receiver<String> receiver) {
        super(URI.create(serverUri));
        this.receiver=receiver;

    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribeMessage = "{ \"eventName\":\"subscribe\", \"eventData\": { \"authToken\": \"8ac97bbb7a402652f7fa47d6d28f32e3caa9337a\" } }";
        send(subscribeMessage);
    }

    @Override
    public void onMessage(String message) {
        // Called when a message is received from the WebSocket
        receiver.store(message);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        // Called when the WebSocket connection is closed
        // You may want to handle reconnection logic here
    }

    @Override
    public void onError(Exception ex) {
        // Called when an error occurs
        // You may want to handle error scenarios here
    }
}

