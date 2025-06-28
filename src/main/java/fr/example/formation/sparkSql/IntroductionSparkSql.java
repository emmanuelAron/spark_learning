package fr.example.formation.sparkSql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;

import java.util.List;

public class IntroductionSparkSql {

    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("IntroductionSparkSql")
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
                                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        }
                )
        );

        //Sql request on the DataFrame
        dataFrame.createOrReplaceTempView("datas");
        Dataset<Row> resultSQL = spark.sql("SELECT * FROM datas WHERE id>=2 LIMIT 1");
        resultSQL.show();
        resultSQL.explain(true);

        Dataset<Row> resultDf = dataFrame.filter(col("id").geq(2))
                .select("id")
                .limit(1);


        //stop SparkSession
        spark.stop();
    }
}
