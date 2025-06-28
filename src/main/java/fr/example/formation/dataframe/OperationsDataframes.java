package fr.example.formation.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class OperationsDataframes {
    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrame operations")
                .master("local[*]")
                .getOrCreate();

        //create Datafremes for simulated datas
        Dataset<Row> df = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "John", 30),
                        RowFactory.create(2, "Alice", 25),
                        RowFactory.create(3, "Bob", 35),
                        RowFactory.create(4, "Maxime", 25),
                        RowFactory.create(5, "Alice", 40)
                ),
                new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("age", DataTypes.IntegerType, true, Metadata.empty())
                })
        );

        //Transformations operations
        Dataset<Row> filteredDF = df.filter(new Column("age").gt(25));
        Dataset<Row> filteredDF2 = df.filter("age > 25");

        filteredDF.show();
        filteredDF2.show();

        //group by ops
        Dataset<Row> groupedDF = df.groupBy("age").count();
        groupedDF.show();
        
        //select
        Dataset<Row> selectDF = df.select("id", "name");
        selectDF.show();

        //distinct
        Dataset<Row> distinctDF = df.select("name").distinct();
        distinctDF.show();
        
        //actions operations
        long count = df.count();
        System.out.println("columns number: "+count);

        spark.stop();
    }
}
