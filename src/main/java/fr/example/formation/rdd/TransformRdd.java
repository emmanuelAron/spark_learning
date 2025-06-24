package fr.example.formation.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.jdk.CollectionConverters;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.List;

public class TransformRdd {

    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("RDDTransformationExample")
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

        //transformation map : multiply every element by 2
        JavaRDD<Integer> mappedRDD = rdd.map(x -> x * 2);
        System.out.println("Transformation map:");
        mappedRDD.collect().forEach(System.out::println);

        //transfo filter : elements > 3
        JavaRDD<Integer> filteredRDD = rdd.filter(x -> x > 3);
        System.out.println("Filter map:");
        filteredRDD.collect().forEach(System.out::println);

        //flatMap transfo : split every elements into words
        JavaRDD<String> wordsRDD = rdd.flatMap(x -> Arrays.asList(String.valueOf(x), "a").iterator());
        System.out.println("FlatMap transfo:");
        wordsRDD.collect().forEach(System.out::println);

        //distinct (remove duplicates)
        JavaRDD<Integer> distinctRDD = rdd.distinct();
        System.out.println("distinct transfo:");
        distinctRDD.collect().forEach(System.out::println);

        //close
        spark.stop();
    }
}
