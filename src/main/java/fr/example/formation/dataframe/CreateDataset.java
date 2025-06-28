package fr.example.formation.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class CreateDataset {
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
                        RowFactory.create(3, "Bob")
                ),
                new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty())
                })
        );
        testDataDF.printSchema();
        testDataDF.show(false);

        //Create from CSV files
        Dataset<Row> csvDF =spark
                .read()
                .option("header","true")
                .csv("src/main/resources/java/dataframe.QueryOptimizations.client.csv");
        csvDF.show();

        //Create from DB
        spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/database")
                .option("dbtable", "table")
                .option("user", "username")
                .option("password","password")
                .load();

        //Create from Parquet files
        String parquetFilePath = "path/to/parquet/file.parquet";
        Dataset<Row> parquetDF = spark.read().parquet(parquetFilePath);

        //Create from ORC files
        String orcFilePath = "path/to/parquet/orc.parquet";
        Dataset<Row> orcDF = spark.read().parquet(orcFilePath);

        spark.stop();
    }
}
