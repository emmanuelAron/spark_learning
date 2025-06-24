package fr.example.formation.installation;

import org.apache.spark.sql.SparkSession;

public class Installation {
    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSessionSparkLearning")
                .master("local[*]")
                .getOrCreate();

        //print version
        String sparkVersion = spark.version();
        System.out.println("Version de spark: "+ sparkVersion);

        //fermer
        spark.stop();
    }
}
