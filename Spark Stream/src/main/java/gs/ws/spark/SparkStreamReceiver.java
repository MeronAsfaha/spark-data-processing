package gs.ws.spark;

import gs.ws.client.CryptoWebSocketClient;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.java_websocket.client.WebSocketClient;


public class SparkStreamReceiver extends Receiver<String> {

    private final String url;
    private WebSocketClient client;

    public SparkStreamReceiver(String url) {
        super(StorageLevel.MEMORY_ONLY());
        this.url = url;
    }

    @Override
    public void onStart() {
        // Start the WebSocket client
        client = new CryptoWebSocketClient(url,this);
        client.connect();
    }

    @Override
    public void onStop() {
        // Stop the WebSocket client
        if (client != null) {
            client.close();
        }
    }
}
