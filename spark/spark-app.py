from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaVideoAudioProcessor").getOrCreate()

video_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "video-stream")
    .load()
)

audio_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "audio-stream")
    .load()
)

video_df.writeStream.format("console").start()
audio_df.writeStream.format("console").start()

spark.streams.awaitAnyTermination()
