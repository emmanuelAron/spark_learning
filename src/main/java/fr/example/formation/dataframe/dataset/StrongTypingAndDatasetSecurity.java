package fr.example.formation.dataframe.dataset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.deploy.Client$;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class StrongTypingAndDatasetSecurity {
    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("StrongTypingAndDatasetSecurity")
                .master("local[*]")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("clientId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, true, Metadata.empty())
        });

        Dataset<Client> clientDataset = spark
                .read()
                .option("header", "true")
                .schema(schema)
                .csv("src/main/resources/java/dataset/client.csv")
                .as(Encoders.bean(Client.class));

        clientDataset.show();
        clientDataset.printSchema();

        //Filter the clients (Strong typing)
        Dataset<Client> adultsDS = clientDataset.filter((FilterFunction<Client>) client -> client.getAge() >= 18);
        adultsDS.show();

        //Version 2
        Dataset<Client> adultsDS2 = clientDataset.filter(Client::isLegalAge);
        adultsDS2.show();


        spark.stop();
    }
}
