package fr.example.formation.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.Window;


import java.awt.*;
import java.util.List;

public class AggregationDataframes {
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
                        RowFactory.create( "John", 30,"USA"),
                        RowFactory.create( "Alice", 25,"France"),
                        RowFactory.create( "Bob", 35,"USA"),
                        RowFactory.create( "Maxime", 25,"France"),
                        RowFactory.create( "Alice", 40,"USA"),
                        RowFactory.create( "Mohamed", 22,"France")
                ),
                new StructType(new StructField[]{

                        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("pays", DataTypes.StringType, true, Metadata.empty())
                })
        );
        //age min
        df.groupBy("pays")
                        .min("age")
                        .show();

        //age max
        df.groupBy("pays")
                .max("age")
                .show();

        //aggregation
        df.groupBy("pays")
                        .agg(
                                functions.min("age").as("age minimum"),
                                functions.max("age").as("age maximum"),
                                functions.count("name").as("nb personne"),
                                functions.avg("age").as("age moyen")
                        ).show();

        //2 younger people
        WindowSpec window = Window.partitionBy("pays").orderBy("age");
        //row_number : rating of the row in the window functions
        df.withColumn("classement_age", functions.row_number().over(window)).show();

        //rank
        System.out.println("function rank");
        df.withColumn("classement_age", functions.rank().over(window)).show();

        //meme chose qu'en SQL, l'age de la personne plus jeune juste avant
        System.out.println("function lag");//permet de comparer les rows entre elles.
        df.withColumn("classement_age", functions.lag("age",1).over(window)).show();

        //average age
        WindowSpec windowPays = Window.partitionBy("pays");
        df.
                withColumn("age_moyen", functions.avg(new Column("age")).over(windowPays))
                .withColumn("age_max", functions.max(new Column("age")).over(windowPays))
                .withColumn("age_min", functions.min(new Column("age")).over(windowPays))
                .withColumn("age_somme", functions.sum(new Column("age")).over(windowPays))
                .show();

        spark.stop();
    }
}
