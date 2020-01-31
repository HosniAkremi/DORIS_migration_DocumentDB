package com.everis.doris
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
object consultations {
  def main(args: Array[String]): Unit ={
   val spark = SparkSession.
     builder.
     master("local").
     appName("consultations").
     getOrCreate()


    //Program paths
    val cores = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/cores.json"
    val user_total_answerF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/user_total_answers.json"
    val pub_consF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/public_consultations.json"

    //Unit Test Paths
//    val cores = "/Users/hosniakremi/Desktop/test_consult/cores"
//    val user_total_answerF = "/Users/hosniakremi/Desktop/test_consult/user_total_answers"
//    val pub_consF = "/Users/hosniakremi/Desktop/test_consult/public_consultations"

    val utaDF = spark.read.json(user_total_answerF).select(
      col("public_consultation_id").as("joinId"),
      col("questions_answers_list"))


    val pub_consDF = spark.read.json(pub_consF).select(
      col("public_consultation_id").as("pub_consId"),
      col("question_number"),
      col("question_text"),
      col("is_open"))

    val c_uta_pubDF = spark.read.json(cores)
      .join(utaDF, col("joinId") === col("displayName"))
      .join(pub_consDF, col("pub_consId") === col("displayName"))
      .withColumn("kind", when(col("type") === "OPC_LAUNCHED", "EUSurvey").otherwise("BRP"))
      .withColumn("owners", explode(col("owners")))
      .withColumn("questions_answers_list", explode(col("questions_answers_list")))
      .withColumn("order", explode(col("sets.order")))
      .withColumn("author",
        struct(
          col("owners").as("name")))
      .withColumn("groups",
        struct(
            col("order").as("id"),
          when(size(col("sets.questions")) ===  lit(1), "section")
            .otherwise(
            when(col("is_open") === "true", "open")
              .otherwise(
              "close")).as("type"),
          struct(
              concat(col("joinId"), lit("_") ,col("questions_answers_list.question_number")).as("id"),
               col("question_text").as("text"),
                lit("question_number").as("order"),
                when(col("is_open") === "true", "open").otherwise("close").as("type"),
              struct(
                col("questions_answers_list.answer_text").as("text")).as("answers")
              ).as("questions")
              )
          )
      .withColumn("audit",
        struct(
          lit("BATCH").as("uploadedBy")
        ))
      .select(
        col("name").as("consultationId"),
        col("displayName").as("alias"),
        col("title").as("title"),
        col("shortName").as("shortName"),
        col("type").as("type"),
        col("startDate").as("startDate"),
        col("endDate").as("endDate"),
        col("modifiedDate").as("modifiedDate"),
        col("kind"),
        col("units").as("units"),
        col("totalFeedbacks").as("totalFeedbacks"),
        col("feedbackStatus").as("status"),
        col("author"),
        col("groups"),
        col("audit")
      )
    c_uta_pubDF.printSchema()
//    c_uta_pubDF.show(false)
    c_uta_pubDF.repartition(1).write.format("json").mode("overwrite").save("/Users/hosniakremi/Desktop/Doris+_migration_data/consultations")
//  df.write.json("/tmp/consultation.json")
}
}