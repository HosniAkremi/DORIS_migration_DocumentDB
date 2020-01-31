package com.everis.doris
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import shaded.parquet.org.codehaus.jackson.annotate.JsonTypeInfo.As
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io._
object users {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder.
      master("local").
      appName("users").
      getOrCreate()
    //Program paths
    val coresF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/cores.json"
    val user_total_answersF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/user_total_answers.json"
   //Unit test paths
//   val coresF = "/Users/hosniakremi/Desktop/test_consult/cores"
//    val user_total_answersF = "/Users/hosniakremi/Desktop/test_consult/user_total_answers"

    val c_DF = spark.read.json(coresF)
      .select(col("displayName").as("joinId"),
      col("name").as("consultationId"),
      col("questions_metadata"))
    println(c_DF.count())

    import spark.implicits._
    val countries_schema = StructType(Seq(
      StructField("geometry", StringType, true), StructField("country", StringType, true)
    ))
    val uta = spark.read.json(user_total_answersF)

    val uta_join_c_DF = spark.read.json(user_total_answersF).join(broadcast(c_DF), col("joinId") === col("public_consultation_id"))
      .withColumn("questions_answers_list", explode(col("questions_answers_list")))
      .withColumn("userId", when(col("questions_metadata.caseId") === col("questions_answers_list.question_number"), col("questions_answers_list.answer_text")))
      .withColumn("countries", from_json($"user_info.countries", countries_schema))
      .select(
        col("userId"),
        col("user_info.name").as("name"),
        col("user_info.email").as("email"),
        col("countries.country").as("country"),
        col("consultationId"),
        col("_id.$oid").as("feedbackId"),
        col("user_info.ind_type").as("userType"))
    uta_join_c_DF.printSchema()

    val a = uta_join_c_DF.schema.json
//    uta_join_c_DF.coalesce(1).write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/users/")
//    val writer = new PrintWriter(new File("/Users/hosniakremi/Desktop/Doris+_migration_data/users/"))
//    writer.write(a)
//    writer.close()

  }
}
