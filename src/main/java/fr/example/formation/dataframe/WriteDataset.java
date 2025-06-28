package fr.example.formation.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Properties;

public class WriteDataset {
    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrame creation")
                .master("local[*]")
                .getOrCreate();

        //create Datafremes for units tests
        Dataset<Row> testDataDF = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "John"),
                        RowFactory.create(2, "Alice"),
                        RowFactory.create(3, "Bob"),
                        RowFactory.create(4, "Pierre")
                ),
                new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty())
                })
        ).repartition(1);

        //text file
        testDataDF.select("name")
                        .write()
                        .mode(SaveMode.Overwrite)
                        .text("src/main/resources/java/dataframe/WriteDataframes/text");

        //json file
        testDataDF
                .write()
                .mode(SaveMode.Append)
                .json("src/main/resources/java/dataframe/WriteDataframes/json");

        //Parquet file
        testDataDF
                .write()
                .mode(SaveMode.Ignore)
                .parquet("src/main/resources/java/dataframe/WriteDataframes/parquet");

        //csv file
        testDataDF
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("id")
                .csv("src/main/resources/java/dataframe/WriteDataframes/csv");

        //BDD
        testDataDF.write()
                        .jdbc("jdbc:postgresql://localhost/test?user=fred&password=secret", "client", new Properties());

        spark.stop();
    }
}
