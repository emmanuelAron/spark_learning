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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

public class AdvancedFunctionsSparkSql {

    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("AdvancedFunctionsSparkSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataFrame = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "John",21),
                        RowFactory.create(2, "Alice",52),
                        RowFactory.create(3, "Bob",40)
                ),
                new StructType(
                        new StructField[]{
                                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                                new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),
                        }
                )
        );

        Dataset<Row> customerCountry = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "Canada"),
                        RowFactory.create(2, "Maroc"),
                        RowFactory.create(3, "Canada")
                ),
                new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("country", DataTypes.StringType, true, Metadata.empty()),
                })
        );



        //Sql request on the DataFrame
        dataFrame.createOrReplaceTempView("customer");
        customerCountry.createOrReplaceTempView("country");

        Dataset<Row> resultSQL = spark.sql("SELECT C.country,SUM(CU.age) AS totalAge" +
                " FROM customer CU " +
                "JOIN country C " +
                "ON CU.id = C.id GROUP BY C.country" );
        resultSQL.show();
        resultSQL.explain(true);

        //User defined function
        UserDefinedFunction random = udf(
                ()-> UUID.randomUUID().toString(),DataTypes.StringType
        );
        random.asNondeterministic();
        spark.udf().register("randomUUID",random);

        resultSQL = spark.sql("SELECT randomUUID() as uuid, C.country,SUM(CU.age) AS totalAge" +
                " FROM customer CU " +
                "JOIN country C " +
                "ON CU.id = C.id GROUP BY C.country" );
        resultSQL.show(false);

        //Udf with parameter
        UserDefinedFunction more5year = udf((UDF1<Integer,Integer>) age-> age+5,DataTypes.IntegerType);
        spark.udf().register("more5year",more5year);
        spark.sql("SELECT *,more5year(age) AS yearPlus5year FROM CUSTOMER").show();


        //stop SparkSession
        spark.stop();
    }
}
