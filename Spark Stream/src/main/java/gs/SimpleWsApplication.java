package gs;


import gs.ws.spark.SparkStreamReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class SimpleWsApplication {

    public static void main(String[] args) {
        // Create a SparkConf and StreamingContext with a batch interval of 10 second
        SparkConf conf = new SparkConf().setAppName("WebSocketSparkStreaming").setMaster("local[*]");

        try (JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10))) {
            HiveContext hiveContext = new HiveContext(jssc.sparkContext());
            // Define the WebSocket endpoint
            String url = "wss://api.tiingo.com/crypto";

            ObjectMapper objectMapper = new ObjectMapper();

            // Create a custom Receiver for WebSocket
            JavaReceiverInputDStream<String> webSocketReceiver = jssc.receiverStream(
                    new SparkStreamReceiver(url)
            );

            // Perform transformations or actions on the DStream
            JavaDStream<String> processedStream = webSocketReceiver.transform(
                    (Function<JavaRDD<String>, JavaRDD<String>>) rdd -> {
                        // Process each RDD here
                        return rdd.filter(line -> {
                            JsonNode node = objectMapper.readTree(line);
                            return node.get("messageType").asText().equals("A");
                        }).map(line -> {
                            // Perform operations on each line
                            JsonNode jsonNode = objectMapper.readTree(line);
                            JsonNode dataArray = jsonNode.get("data");
                            return dataArray.toString().replace("[", "").replace("]", "");
                        });
                    }
            );
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("messageType", DataTypes.StringType, true),
                    DataTypes.createStructField("ticker", DataTypes.StringType, true),
                    DataTypes.createStructField("date", DataTypes.StringType, true),
                    DataTypes.createStructField("exchange", DataTypes.StringType, true),
                    DataTypes.createStructField("lastSize", DataTypes.FloatType, true),
                    DataTypes.createStructField("lastPrice", DataTypes.FloatType, true),
            });

            processedStream.transform(rdd -> rdd.map((line) -> {
                        String[] data = line.split(",");
                        return RowFactory.create(
                                data[0],
                                data[1],
                                data[2],
                                data[3],
                                Float.parseFloat(data[4]),
                                Float.parseFloat(data[5])
                        );

                    }).filter(row -> row.get(3).toString().contains("binance"))
                    .filter((row)-> Float.parseFloat(row.get(5).toString()) > 50.1)).foreachRDD(rdd -> {
                DataFrame dataFrame = hiveContext.createDataFrame(rdd, schema);
                dataFrame.write().mode(SaveMode.Append).saveAsTable("crypto_table");
            });


            // Save the processed data
//            processedStream.dstream()
//            .saveAsTextFiles("hdfs://localhost:8020/user/cloudera/sparkStreamOutput/output", "");

            // Start the Spark Streaming context
            jssc.start();

            // Wait for the termination of the context
            jssc.awaitTermination();
        }
    }
}