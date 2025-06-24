package fr.example.formation.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.jdk.CollectionConverters;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.List;

public class CreateRdd {

    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("RDDCreationExample")
                .master("local[*]")
                .getOrCreate();

        /*
            local datas
         */
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        //RDD Creation from local datas with the parallelize method (java to scala conversion)
        //numslices = 1 (partition number)
        //RDD of integers
        JavaRDD<Integer> localDataRDD = spark.sparkContext().parallelize(
                CollectionConverters.IteratorHasAsScala(data.iterator()).asScala().toSeq(),
                1,
                ClassTag$.MODULE$.apply(Integer.class)
        ).toJavaRDD();

        //print RDD datas
        System.out.println("Datas RDD created from locales datas:");
        localDataRDD.collect().forEach(System.out::println);

        /*
        Local
         */
        String localTextFilePath = "src/main/resources/java/rdd/CreateRdd/prenom.txt";
        //RDD creation from file text on HDFS with textFile method
        JavaRDD<String> localTextFileRDD = spark.sparkContext().textFile(localTextFilePath, 1).toJavaRDD();
        System.out.println("\nRDD datas created from file on the FS:");
        localTextFileRDD.take(5).forEach(System.out::println);//print the first 5 lines

        /**
         * HDFS (or S3...)
         */
        //path file on hdfs
         String hdfsFilePath = "hdfs://path/to/yourFile.txt";
        JavaRDD<String> hdfsTextFileRDD = spark.sparkContext().textFile(localTextFilePath, 1).toJavaRDD();
        System.out.println("\nRDD datas created from file on the FS:");
        localTextFileRDD.take(5).forEach(System.out::println);//print the first 5 lines


        //close
        spark.stop();
    }
}
