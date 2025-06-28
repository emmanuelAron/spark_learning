package fr.example.formation.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;

import java.util.List;

public class Enonce {
    public static void main(String[] args) {
        //spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrame operations")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csvDF = spark.read().csv("src/main/resources/java/dataframe/Exercice/pokemonInfo.csv");
        csvDF.show();

        StructType schemaInfo = new StructType(
                new StructField[]{
                        new StructField("id",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("Name",DataTypes.StringType,true,Metadata.empty()),
                        new StructField("Type1",DataTypes.StringType,true,Metadata.empty()),
                        new StructField("Type2",DataTypes.StringType,true,Metadata.empty()),
                        new StructField("Total",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("Generation",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("Legendary",DataTypes.BooleanType,true,Metadata.empty())
                }
        );

        Dataset<Row> pokemonDF = spark.read().schema(schemaInfo).option("header","true").csv("src/main/resources/java/dataframe/Exercice/pokemonInfo.csv");
        //legendary pokemon of first generation
        Dataset<Row> legendaryFirstGen = pokemonDF.filter("Legendary == true AND Generation == 1");
        legendaryFirstGen.show(false);

        //how many fire pokemons?
        long countFireP = pokemonDF.filter("Type1 == 'Fire' OR Type2 == 'Fire'").count();
        System.out.println("how many fire pokemons? "+countFireP);

        //Print every types of uniques Pokemons present in the data set.
        pokemonDF.select("Type1").union(pokemonDF.select("Type2")).distinct().show(false);

        //Find HP,Attack,defense,special attack,special defense,speed
        //for every Pokemons of 'water' type
        System.out.println("Find HP,Attack,defense,special attack,special defense,speed\n" +
                "        for every Pokemons of 'water' type");
        StructType schemaStats = new StructType(
                new StructField[]{
                        new StructField("id",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("Name",DataTypes.StringType,true,Metadata.empty()),
                        new StructField("Total",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("HP",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("Attack",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("Defense",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("AttackSpecial",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("DefenseSpecial",DataTypes.IntegerType,true,Metadata.empty()),
                        new StructField("Speed",DataTypes.IntegerType,true,Metadata.empty())
                }
        );
        Dataset<Row> pokemonStats = spark.read().schema(schemaStats).option("header", "true").csv("src/main/resources/java/dataframe/Exercice/pokemonStats.csv");

        Dataset<Row> waterPokem = pokemonDF
                .filter("Type1 == 'Water' OR Type2 == 'Water' ");
                //.withColumnRenamed("Name","NameWater");

        System.out.println("waterPokem Dataset");
        waterPokem.show();

        //pokemonDF.show();
        Dataset<Row> statsPoke = pokemonStats.select("id","name", "HP", "Attack", "Defense", "DefenseSpecial", "AttackSpecial", "Speed");
        System.out.println("statsPoke Dataset");
        statsPoke.show();

        //join between water type pokemon and stats of pokemon using id and name
        Dataset<Row> joinedWaterPokemon = waterPokem.join(statsPoke,
                waterPokem.col("id").equalTo(statsPoke.col("id")).and(waterPokem.col("Name").equalTo(statsPoke.col("Name")))
        );
        joinedWaterPokemon.show();
        //waterPokem.join(statsPoke)

        //Print 5 water pokemon with less speed
        Dataset<Row> sorted5WaterLessSpeed = joinedWaterPokemon.orderBy(new Column("Speed").asc())
                .limit(5);

        sorted5WaterLessSpeed.show();

        //Add a new column "TotalDefense" to the statsPokemon which represent the sum of Defense and DefenseSpecial
        System.out.println("Add a new column \"TotalDefense\" to the statsPokemon which represent the sum of Defense and DefenseSpecial");
        pokemonStats.withColumn("TotalDefense",
                new Column("Defense").plus(new Column("DefenseSpecial"))
                ).show();

        System.out.println("New column version 2");
        Dataset<Row> pokemonWithTotalDefense = pokemonStats.withColumn("TotalDefense",
                col("Defense").plus(col("DefenseSpecial")));
        pokemonWithTotalDefense.show();

        //Print execution plan of last question
        pokemonWithTotalDefense.explain(true);

        //How many pokemons have 2 types?
        //Filter pokemons with one unique type
        Dataset<Row> doubleTypePokemon = pokemonDF.na().drop(new String[]{"Type2"});

        //Count Pokemon number with 2 types
        long doubleTypePokemonCount = doubleTypePokemon.count();
        System.out.println("Number of Pokemons with double type: "+doubleTypePokemonCount);

        spark.stop();
    }
}
