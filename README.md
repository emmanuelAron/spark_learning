# 🚀 Spark Learning (Java)

Projet de formation pour apprendre **Apache Spark** en **Java**, basé sur les mêmes concepts qu'en **Scala** et **PySpark**.  
L’objectif est de manipuler les principales fonctionnalités de Spark : **DataFrames**, **RDDs**, **SparkSQL**, **Streaming** et l'utilisation de **Databricks**.

---

## 📌 Contenu du dépôt

### **1. DataFrames**
Exercices pratiques pour créer, manipuler et agréger des **DataFrames**.

- `CreateDataset.java` → création de DataFrames à partir de **collections**, **CSV**, **Parquet**, **ORC**, **BDD**.
- `OperationsDataframes.java` → filtres, sélections, regroupements, distinct.
- `AggregationDataframes.java` → agrégations complexes avec **min**, **max**, **count**, **avg**, **fenêtres**.
- `WriteDataset.java` → export des DataFrames en **CSV**, **JSON**, **Parquet**, **BDD**.
- `Enonce.java` → exercices complets sur des données Pokémon.

---

### **2. RDDs**
Introduction aux **Resilient Distributed Datasets** (RDD).

- `CreateRdd.java` → création d’un RDD à partir de listes.
- `ActionRDD.java` → actions Spark (`count`, `take`, `collect`, `saveAsTextFile`).
- `TransformRdd.java` → transformations (`map`, `filter`, `flatMap`).
- `Enonce.java` → exercices pratiques.

---

### **3. SparkSQL**
- Exécution de requêtes SQL directement sur les **DataFrames**.
- Gestion des schémas et des vues temporaires.

---

### **4. Structured Streaming**
- Introduction au **streaming** avec Spark.
- Lecture et traitement de données **en temps réel**.

---

## 🛠️ Technologies utilisées

- **Java 17**
- **Apache Spark 3.x**
- **Maven**
- **Databricks**
- Formats de fichiers : CSV, JSON, Parquet, ORC

---

## 🎯 Objectifs pédagogiques

- Comprendre les concepts fondamentaux de **Spark**.
- Manipuler **DataFrames**, **RDD** et **SparkSQL**.
- Découvrir le **Structured Streaming**.
- S’entraîner pour la certification **Databricks Certified Associate Developer for Apache Spark 4**.

---

## 🧩 Structure du projet

```bash
spark_learning/
│── src/main/java/fr/example/formation/
│   ├── dataframe/
│   ├── rdd/
│   ├── sparkSql/
│   ├── streaming/
│── resources/java/
│── pom.xml
│── README.md

