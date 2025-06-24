package fr.example.formation.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.jdk.CollectionConverters;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.List;

public class ActionRDD {

    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("RDDActionExample")
                .master("local[*]")
                .getOrCreate();

        /*
            local datas
         */
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        //Create a RDD from a list
        JavaRDD<Integer> rdd = spark.sparkContext().parallelize(
                CollectionConverters.IteratorHasAsScala(data.iterator()).asScala().toSeq(),
                1,
                ClassTag$.MODULE$.apply(Integer.class)
        ).toJavaRDD();

        //Count elements (action)
        long count = rdd.count();
        System.out.println("Number of elements into rdd: " + count);

        //Collect RDD elements as local array (action)
        List<Integer> collected = rdd.collect();
        System.out.println("Rdd content: "+collected.toString());

        //Take the first elements of rdd (Action)
        List<Integer> taken = rdd.take(3);
        System.out.println("The first 3 elements of RDD: "+taken.toString());

        String destination = "src/main/resources/java/rdd/RDDActionsExample/saveAsTextFile2";
        //Save RDD as a text file
        rdd.saveAsTextFile(destination);

        //close
        spark.stop();
    }
}
