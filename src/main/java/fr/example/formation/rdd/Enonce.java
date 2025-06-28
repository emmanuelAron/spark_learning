package fr.example.formation.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Enonce {

    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Enonce")
                .master("local[*]")
                .getOrCreate();

        String filePath = "src/main/resources/java/rdd/Exercice/Popular_Spotify_Songs.csv";

        JavaRDD<String> spotifyRDD = spark.sparkContext().textFile(filePath, 3).toJavaRDD();

        //combien de titres differents répertoriés?
        JavaRDD<String> titles = spotifyRDD.map(line -> line.split(",")[0]);//get the first column
        long count = titles.distinct().count();
        System.out.println(count);//468

        //combien de artistes differents répertoriés?
        JavaRDD<String> artists = spotifyRDD.map(line -> line.split(",")[1]);//get the second column
        long artistCount = artists.distinct().count();
        System.out.println(artistCount);

        //citer 10 morceaux de musique sortis en 2014
        //annee
        JavaRDD<String> titles2014 = spotifyRDD.filter(line -> line.split(",")[3].equals("2014"))
                .map(line ->line.split(",")[0]);

        List<String> top10 = titles2014.take(10);
        top10.forEach(System.out::println);


        //close
        spark.stop();
    }
}
