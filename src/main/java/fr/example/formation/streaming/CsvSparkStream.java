package fr.example.formation.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.uuid;

public class CsvSparkStream {

    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("CsvSparkStream")
                .master("local[*]")
                .getOrCreate();

        StructType csvSchema = new StructType()
                .add("name", "string")
                .add("age", "integer");

        Dataset<Row> csvDF = spark.readStream()
                .option("header", true)
                .option("sep", ";")
                .schema(csvSchema)
                .csv("src/main/resources/java/streaming/csvSparkStream/in");

        if(csvDF.isStreaming()){
            csvDF.printSchema();
        }
        csvDF.createOrReplaceTempView("updates");

        StreamingQuery query = null;
        try {
            query = spark
                    .sql("select count(*) as nbLigne from updates")
                    .writeStream()
                    .outputMode("complete")
                    .format("console")
                    .start();
            query.awaitTermination();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }


        //stop SparkSession
        spark.stop();
    }
}
