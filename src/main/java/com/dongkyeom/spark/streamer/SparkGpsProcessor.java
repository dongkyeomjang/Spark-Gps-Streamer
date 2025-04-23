package com.dongkyeom.spark.streamer;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

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
                .add("latitude", DataTypes.DoubleType)
                .add("longitude", DataTypes.DoubleType)
                .add("timestamp", DataTypes.LongType);

        // Kafka 스트리밍 데이터 읽기
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
                .option("subscribe", "raw-gps")
                .option("startingOffsets", "earliest")
                .load();

        // Kafka 메시지 파싱
        Dataset<Row> parsed = kafkaStream
                .selectExpr("CAST(key AS STRING) as trip_id_key", "CAST(value AS STRING) as json_value")
                .select(
                        col("trip_id_key"),
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
                // timestamp(long) → timestamp 타입으로 변환
                .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp").divide(1000))));

        // Watermark 설정 (지연 허용 시간)
        Dataset<Row> gpsDataWithWatermark = parsed
                .withWatermark("timestamp", "10 minutes");

        // trip_id별로 카운트하여 2개 이상인 경우만 필터링
        Dataset<Row> filtered = gpsDataWithWatermark
                .groupBy("trip_id", "trip_id_key", "agent_id", "latitude", "longitude", "timestamp")
                .count()
                .filter("count >= 2")
                .drop("count");

        // HBase에 저장
        StreamingQuery query = filtered.writeStream()
                .foreach(new ForeachWriter<Row>() {

                    private HBaseWriter writer;

                    @Override
                    public boolean open(long partitionId, long version) {
                        try {
                            writer = new HBaseWriter();
                            return true;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return false;
                        }
                    }

                    @Override
                    public void process(Row row) {
                        try {
                            String tripId = row.getAs("trip_id");
                            String json = row.json();
                            writer.writeToHBase(tripId, json);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        try {
                            if (writer != null) {
                                writer.close();
                            }
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
