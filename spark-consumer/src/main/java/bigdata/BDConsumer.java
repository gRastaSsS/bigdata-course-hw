package bigdata;

import common.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.window;

public final class BDConsumer {
    private BDConsumer() {
    }

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        String usersPath = args[0];
        String windowDuration = args[1];
        boolean outputToConsole = Boolean.parseBoolean(args[2]);

        SparkConf conf = new SparkConf()
            .setAppName("bd_task_2_app")
            .set("spark.sql.streaming.checkpointLocation", "/tmp/spark/checkpoints")
            .setMaster("local[2]");

        SparkSession spark = SparkSession.builder()
            .config(conf)
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> posts = spark
            .readStream()
            .format("kafka")
            .option("kafka." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS)
            .option("startingoffsets", "latest")
            .option("subscribe", Constants.TOPIC)
            .load();

        DataType postSchema = DataType.fromJson(Constants.DDL);

        Dataset<Row> structPosts = posts
            .select(col("value").cast(DataTypes.StringType))
            .select(from_json(col("value"), postSchema).as("post"));

        Dataset<Row> users = spark.read().json(usersPath);

        Dataset<Row> filteredData = structPosts
            .join(users, col("post.from_id").equalTo(col("id")))
            .select(
                col("post.id").alias("post_id"),
                udf(Utils.PUNCTUATION_REMOVER, DataTypes.StringType).apply(col("post.text")).alias("post_text"),
                col("id").alias("user_id"),
                col("sex"),
                udf(Utils.AGE_CALCULATOR, DataTypes.IntegerType).apply(col("bdate")).alias("age")
            )
            .withColumn("timestamp", current_timestamp());

        Tokenizer tokenizer = new Tokenizer()
            .setInputCol("post_text")
            .setOutputCol("tokenized_words");

        StopWordsRemover remover = new StopWordsRemover()
            .setStopWords(StopWordsRemover.loadDefaultStopWords("russian"))
            .setInputCol("tokenized_words")
            .setOutputCol("words_without_stopwords");

        Dataset<Row> tokenizedTextData = tokenizer.transform(filteredData);

        Dataset<Row> tokenizedTextDataWithoutStopwords = remover.transform(tokenizedTextData);

        Dataset<Row> filteredWords = tokenizedTextDataWithoutStopwords
            .withColumn("word", explode(col("words_without_stopwords")))
            .drop("words_without_stopwords", "tokenized_words", "post_text")
            .drop("post_id", "user_id")
            .filter(col("word").notEqual(""));

        Bucketizer bucketizer = new Bucketizer()
            .setInputCol("age")
            .setOutputCol("age_range_id")
            .setSplits(new double[] { 0, 18, 27, 40, 60, 120 });

        Dataset<Row> ageRangedData = bucketizer.transform(filteredWords);

        Dataset<Row> windowedCounts = ageRangedData.groupBy(
            window(col("timestamp"), windowDuration, windowDuration),
            col("sex"),
            col("age_range_id")

        ).count();

        if (outputToConsole) {
            StreamingQuery toConsole = windowedCounts
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .option("truncate", "false")
                .start();
        }

        StreamingQuery toKafka = windowedCounts
            .select(to_json(struct("window", "sex", "age_range_id", "count")).alias("value"))
            .writeStream()
            .outputMode(OutputMode.Update())
            .format("kafka")
            .option("kafka.bootstrap.servers", Constants.BOOTSTRAP_SERVERS)
            .option("topic", Constants.OUT_TOPIC)
            .start();

        toKafka.awaitTermination();
        spark.close();
    }
}
