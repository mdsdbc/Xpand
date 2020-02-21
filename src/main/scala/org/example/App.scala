package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._


object App {

  val debug = false
  val rel_path = "src/main/resources/"

  /**
   * Prints a resume of the Data frame current schema and state
   * @param df Data frame to be exposed
   * @param msg additional information (optional)
   */
  def print_state (df : DataFrame,msg: String = "" ): Unit ={
    println(msg)
    df.printSchema()
    df.show(300)
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
  /*def read_csv_2(sc : SparkSession , path : String ): DataFrame ={
    val source = rel_path + path
    val customSchema = StructType(Array(
      StructField("App", StringType, false),
      StructField("Category", StringType, true),
      StructField("Rating", DoubleType, true),
      StructField("Reviews", LongType, true),
      StructField("Size", StringType, true),
      StructField("Installs", StringType, true),
      StructField("Type", StringType, true),
      StructField("Price", StringType, true),
      StructField("Content Rating", StringType, true),
      StructField("Genres", StringType, true),
      StructField("Last Updated", StringType, true),
      StructField("Current Ver", StringType, true),
      StructField("Android Ver", StringType, true),
      StructField("bytes_served", StringType, true))
    )
    val df = sc
      .read
      .option("escape","\"")
      .option("header","true")
      .schema(customSchema)
      .csv(source)
    if(debug){
      print_state(df,"read csv file:"+path)
    }
    df
  }*/

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

    if(!debug){
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

    if(!debug){
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

  def part3( df : DataFrame): Unit = {
    /*val df_1 = df
       //.withColumn("Categories",collect_list("Category"))
      .withColumn("Price",col("Price")*0.9)
      .groupBy("App")
      .agg(collect_list("Category").as("Categories"),avg("Rating").as("Rating"))*/

    val genres: String => String = _.replace(",",";")

    def size(size: String) : Double = {
      size match {
        case size if size.contains("M") => size.split("M")(0).toDouble*1024*1024
        case size if size.contains("K") => size.split("K")(0).toDouble*1024
        case size if size.contains("B") => size.split("B")(0).toDouble
        case _ => Double.NaN
      }
    }

    val genresUDF = udf(genres)
    val sizeUDF = udf(size _)

    /*
    val sizeUDF = udf((size: String ) => ( size) match {
      case size if size.contains("M") => size.split("M")(0).toInt*1024*1024
      case size if size.contains("K") => size.split("M")(0).toInt*1024
      case _ => null
    })
*/


    var df_1 = df.withColumn("Price",col("Price")*0.9)
    df_1 = df_1.withColumn("Genres", genresUDF(col("Genres")))
    df_1 = df_1.withColumn("Size", sizeUDF(col("Size")))

    df_1 = df_1
      .groupBy("App")
      .agg(collect_list("Category").as("Categories"))

    df_1.show(300)
    df_1.printSchema()

  }

  def main (arg: Array[String]): Unit = {
    val cvs_files = Array("googleplaystore_user_reviews.csv","googleplaystore.csv")

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    //part1
    val source_df_1 = read_csv(spark,cvs_files(0))
    val df_1 = part1(source_df_1)
    //part2
    val csv_output_folder = "part2"
    val csv_output_file_name = "best_apps.csv"
    val csv_delimiter = "§"

    fs.delete(new Path(rel_path + csv_output_folder), true)
    val source_df_2 = read_csv(spark,cvs_files(1))
    part2(fs,source_df_2,csv_output_folder,csv_output_file_name,csv_delimiter)
    //part3
    part3(source_df_2)
    spark.stop()
  }
}