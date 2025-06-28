package fr.example.formation.sparkSql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.UUID;

import static org.apache.spark.sql.functions.*;

public class FunctionsSqlSparkDataframe {

    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("AdvancedFunctionsSparkSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataFrame = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "John"),
                        RowFactory.create(2, "Alice"),
                        RowFactory.create(3, "Bob")
                ),
                new StructType(
                        new StructField[]{
                                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("name", DataTypes.StringType, true, Metadata.empty())

                        }
                )
        );
        dataFrame
                .withColumn("UUID",uuid())
                .withColumn("dayDate",current_date())
                .show();

        dataFrame.createOrReplaceTempView("test");

        //Sql request on the DataFrame
        dataFrame.createOrReplaceTempView("test");

        Dataset<Row> resultSQL = spark.sql("SELECT *, current_date() as date from test" );
        resultSQL.show();

        //stop SparkSession
        spark.stop();
    }
}
