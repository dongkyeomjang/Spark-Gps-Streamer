package com.dongkyeom.spark.streamer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.window;

public class SparkGpsProcessor {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaToHBaseGpsProcessor")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Kafka 메시지 value의 JSON 스키마 정의
        StructType schema = new StructType()
                .add("trip_id", DataTypes.StringType)
                .add("agent_id", DataTypes.StringType)
                .add("latitude", DataTypes.StringType)
                .add("longitude", DataTypes.StringType)
                .add("timestamp", DataTypes.StringType);

        // Kafka 스트리밍 데이터 읽기
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29093,kafka-3:29094")
                .option("subscribe", "raw-gps")
                .option("startingOffsets", "latest")          // ← 이 설정이 중요
                .option("failOnDataLoss", "false")            // ← 토픽이 없거나 중간에 사라져도 중단 X
                .load();

        // Kafka 메시지 파싱
        Dataset<Row> parsed = kafkaStream
                .selectExpr("CAST(key AS STRING) as trip_id_key", "CAST(value AS STRING) as json_value")
                .select(
                        col("trip_id_key").cast("long").alias("trip_id_key"),
                        from_json(col("json_value"), schema).alias("data")
                )
                .select(
                        col("trip_id_key"),
                        col("data.trip_id"),
                        col("data.agent_id"),
                        col("data.latitude"),
                        col("data.longitude"),
                        col("data.timestamp")
                )
                .withColumn("trip_id", col("trip_id").cast("long"))
                .withColumn("agent_id", col("agent_id").cast("long"))
                .withColumn("latitude", col("latitude").cast("double"))
                .withColumn("longitude", col("longitude").cast("double"))
                .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"));

        // Watermark 설정 (지연 허용 시간)
        Dataset<Row> gpsDataWithWatermark = parsed
                .withWatermark("timestamp", "10 minutes");

        //trip_id별로 카운트하여 2개 이상인 경우만 필터링
        Dataset<Row> countPerTripId = gpsDataWithWatermark
                .groupBy(
                        col("trip_id"),
                        window(col("timestamp"), "60 minutes")
                )
                .count()
                .filter("count >= 2")
                .select("trip_id");

        //count 2 이상인 trip_id만 원본과 Join
        Dataset<Row> filtered = gpsDataWithWatermark
                .dropDuplicates("trip_id", "timestamp")
                .join(countPerTripId, "trip_id");

        // HBase에 저장
        StreamingQuery query = filtered.writeStream()
                .foreach(new ForeachWriter<Row>() {

                    private HBaseWriter writer;
                    private KafkaTriggerProducer producer;
                    private final Map<String, List<Row>> buffer = new HashMap<>();

                    @Override
                    public boolean open(long partitionId, long version) {
                        try {
                            writer = new HBaseWriter();
                            producer = new KafkaTriggerProducer();
                            return true;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return false;
                        }
                    }

                    @Override
                    public void process(Row row) {
                        try {
                            String tripId = row.getAs("trip_id").toString();

                            // HBase 저장
                            try {
                                writer.writeToHBase(tripId, row.json());
                            } catch (Exception e) {
                                System.err.println("[HBase] ERROR writing to HBase: " + e.getMessage());
                                e.printStackTrace();
                            }

                            // 버퍼에 저장
                            buffer.computeIfAbsent(tripId, k -> new ArrayList<>()).add(row);
                            int size = buffer.get(tripId).size();

                            // 버퍼에 2개 이상 모이면 Kafka로 트리거 메시지 발행
                            if (size >= 2) {
                                List<Row> gpsList = buffer.get(tripId);
                                gpsList.sort(Comparator.comparing(r -> ((java.sql.Timestamp) r.getAs("timestamp")).getTime()));

                                Row fromRow = gpsList.get(0);
                                Row toRow = gpsList.get(1);

                                String message = String.format(
                                        "{ \"from\": {\"trip_id\": \"%s\", \"agent_id\": \"%s\", \"latitude\": %f, \"longitude\": %f}, " +
                                                "\"to\": {\"trip_id\": \"%s\", \"agent_id\": \"%s\", \"latitude\": %f, \"longitude\": %f} }",
                                        fromRow.getAs("trip_id").toString(),
                                        fromRow.getAs("agent_id").toString(),
                                        (Double) fromRow.getAs("latitude"),
                                        (Double) fromRow.getAs("longitude"),
                                        toRow.getAs("trip_id").toString(),
                                        toRow.getAs("agent_id").toString(),
                                        (Double) toRow.getAs("latitude"),
                                        (Double) toRow.getAs("longitude")
                                );

                                try {
                                    producer.send(tripId, message);
                                } catch (Exception e) {
                                    System.err.println("[Kafka] ERROR sending message: " + e.getMessage());
                                    e.printStackTrace();
                                }

                                gpsList.remove(0);
                            }

                        } catch (Exception e) {
                            System.err.println("[process] ERROR in process(): " + e.getMessage());
                            e.printStackTrace();
                        }
                    }


                    @Override
                    public void close(Throwable errorOrNull) {
                        try {
                            if (writer != null) writer.close();
                            if (producer != null) producer.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
                .outputMode("append")
                .start();

        query.awaitTermination();
    }
}
