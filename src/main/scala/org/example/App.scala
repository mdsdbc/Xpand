package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._

object App {

  val debug = true
  val rel_path = "src/main/resources/"

  /**
   * Prints a resume of the Data frame current schema and state
   * @param df Data frame to be exposed
   * @param msg additional information (optional)
   */
  def print_state (df : DataFrame,msg: String = "" ): Unit ={
    println(msg)
    df.printSchema()
    df.show(500)
  }

  /**
   * loads a single csv file into a Dataframe
   * @param sc Current Spark Session
   * @param path relative path to the resources folder
   * @return loaded Dataframe
   */
  def read_csv(sc : SparkSession , path : String ): DataFrame ={
    val source = rel_path + path
    val df = sc
      .read
      .option("escape","\"")
      .option("header","true")
      .csv(source)
    if(debug){
      print_state(df,"read csv file:"+path)
    }
    df
  }

  /**
   * I assumed nan values count as 0 for the average.
   * @param df loaded Dataframe
   * @return transformed DataFrame
   */
  def part1( df : DataFrame ): DataFrame ={
    val df_1 = df
    .na.replace(Seq("Sentiment_Polarity"), Map("nan" -> "0.0"))
    .groupBy("App")
    .agg(
      avg("Sentiment_Polarity").as("Average_Sentiment_Polarity")
    )

    if(debug){
      print_state(df_1,"Part1 - result")
    }
    df_1
  }

  /**
   * Obtain all Apps with a "Rating" greater or equal to 4.0 sorted in descending order.
   * Save that Dataframe as a CSV (delimiter: "§") named "best_apps.csv"
   * @param fs filesystem context to perform rename action
   * @param df loaded Dataframe
   * @param csv_output_folder  folder in the relative path to the resources
   * @param csv_output_file_name file name
   * @param delimiter delimiter value
   */
  def part2(fs : FileSystem , df : DataFrame, csv_output_folder : String, csv_output_file_name : String, delimiter : String ): Unit ={

    val toDbl = udf[Double, String]( _.toDouble)
    var df_1 = df.withColumn("Rating", toDbl(col("Rating")))
    df_1 = df_1
      .na.drop(Seq("Rating"))
      .select("App","Rating")
      .filter("Rating >= 4.0")
      .orderBy(desc("Rating"))

    if(debug){
      print_state(df_1,"Part2 - result")
    }

    df_1
      .coalesce(1)
      .write
      .option("sep",delimiter) // attr: sep alias delimiter
      .option("header","true")
      .csv(rel_path + csv_output_folder)

    val file = fs.globStatus(new Path(rel_path + csv_output_folder + "/part-0000*.csv"))(0).getPath().getName()
    fs.rename(new Path(rel_path + csv_output_folder +"/" + file), new Path(rel_path + csv_output_folder +"/"+csv_output_file_name))
  }

  def part3( df : DataFrame): DataFrame = {

    val toDbl = udf[Double, String]( _.toDouble)
    val toLng = udf[Long, String]( _.toLong)

    val genres: String => Array[String] = _.split(";")

    def size(size: String) : Double = {
      size match {
        case size if size.contains("M") => size.split("M")(0).toDouble
        case size if size.contains("k") => size.split("k")(0).toDouble/1024
        case size if size.contains("B") => size.split("B")(0).toDouble/1024*1024
        case _ => Double.NaN
      }
    }

    val genresUDF = udf(genres)
    val sizeUDF = udf(size _)

    var df_1 = df.withColumn("Rating", toDbl(col("Rating")))
    df_1 = df_1.withColumn("Reviews", toLng(col("Reviews")))
    df_1 = df_1.withColumn("Size", sizeUDF(col("Size")))
    df_1 = df_1.withColumn("Price",col("Price")*0.9)
    df_1 = df_1.withColumn("Genres", genresUDF(col("Genres")))
    // TODO: seems to require a hand made parser with split and switch case for month
    //df_1 = df_1.withColumn("Last Updated", to_date(col("Last Updated"),"MONTH dd, yyyy"))

    df_1 = df_1.withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    val df_2 = df_1
      .groupBy("App")
      .agg(collect_set("Category").as("Categories"))

    /*
    line 10474 - file:googleplaystore.csv - line with bad format, missing field "Category". I added a "," to correct it
    and generate a (null) field since I don't know which category it should be
    */
    var df_3 = df_1
        .groupBy("App")
        .agg(
          max("Reviews").as("Reviews")
        )

    df_3 = df_1.join(
      df_3,
      Seq("App","Reviews"),
      "inner"
    )

    df_3 = df_2.join(
      df_3,
      Seq("App"),
      "inner"
    )

    if(debug){
      print_state(df_3,"Part3 - result")
    }
    df_3
  }

  def part4(fs : FileSystem , df_1 : DataFrame,df_3 : DataFrame, csv_output_folder : String, csv_output_file_name : String): DataFrame= {
    val df_4 = df_1.join(
      df_3,
      Seq("App"),
      "inner"
    )

    if(debug){
      print_state(df_4,"Part4 - result")
    }

    df_4
      .coalesce(1)
      .write
      .option("header","true")
      .parquet(rel_path + csv_output_folder)

    val file = fs.globStatus(new Path(rel_path + csv_output_folder + "/part-0000*.parquet"))(0).getPath().getName()
    fs.rename(new Path(rel_path + csv_output_folder +"/" + file), new Path(rel_path + csv_output_folder +"/"+csv_output_file_name))
    df_4
  }

  def part5(fs : FileSystem ,df_4 : DataFrame, csv_output_folder : String, csv_output_file_name : String): DataFrame= {
    val df_5 = df_4
      .groupBy("Genres")
      .agg(
        count("Genres").as("Count"),
        avg("Rating").as("Average_Rating"),
        avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")
      )

    if(debug){
      print_state(df_5,"Part5 - result")
    }

    df_5
      .coalesce(1)
      .write
      .option("header","true")
      .parquet(rel_path + csv_output_folder)

    val file = fs.globStatus(new Path(rel_path + csv_output_folder + "/part-0000*.parquet"))(0).getPath().getName()
    fs.rename(new Path(rel_path + csv_output_folder +"/" + file), new Path(rel_path + csv_output_folder +"/"+csv_output_file_name))

    if(debug){
      print_state(df_5,"Part4 - result")
    }
    df_5
  }

  def main (arg: Array[String]): Unit = {
    val cvs_files = Array("googleplaystore_user_reviews.csv","googleplaystore.csv")

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .config("spark.sql.parquet.compression.codec", "gzip")
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    //part1
    val source_df_1 = read_csv(spark,cvs_files(0))
    val df_1 = part1(source_df_1)

    //part2
    var csv_output_folder = "part2"
    var csv_output_file_name = "best_apps.csv"
    val csv_delimiter = "§"

    fs.delete(new Path(rel_path + csv_output_folder), true) //clean up

    val source_df_2 = read_csv(spark,cvs_files(1))
    part2(fs,source_df_2,csv_output_folder,csv_output_file_name,csv_delimiter)

    //part3
    val df_3 =  part3(source_df_2)

    //part4
    csv_output_folder = "part4"
    csv_output_file_name = "googleplaystore_cleaned"

    fs.delete(new Path(rel_path + csv_output_folder), true) //clean up

    val df_4 = part4(fs,df_1,df_3,csv_output_folder,csv_output_file_name)

    //part5
    csv_output_folder = "part5"
    csv_output_file_name = "googleplaystore_metrics"

    fs.delete(new Path(rel_path + csv_output_folder), true) //clean up

    part5(fs,df_4,csv_output_folder,csv_output_file_name)

    spark.stop()
  }
}