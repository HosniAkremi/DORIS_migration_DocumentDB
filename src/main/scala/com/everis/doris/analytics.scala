package com.everis.doris

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{struct, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object analytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder.
      master("local").
      appName("analytics").
      getOrCreate()

    spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed",	"true")
    spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize",	"10000")

    //Program paths
    val coresF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/cores.json"
    val user_total_answerF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/user_total_answers.json"
    val pub_consF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/public_consultations.json"
    val q_tot_answerF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/question_total_answers.json"

    //Unit test paths
//    val coresF ="/Users/hosniakremi/Desktop/test_consult/cores"
//    val user_total_answerF = "/Users/hosniakremi/Desktop/test_consult/user_total_answers"
//    val pub_consF= "/Users/hosniakremi/Desktop/test_consult/public_consultations"
//    val q_tot_answerF = "/Users/hosniakremi/Desktop/test_consult/question_total_answers"

    val cDF = spark.read.json(coresF)
      .withColumn("links", explode(col("links")))
      .withColumn("owners", explode(col("owners")))
      .withColumn("consultationId", col("name"))
      .select(col("links.parentId").as("linkedQuestionId"),
        col("consultationId"),
        col("displayName"),
        col("owners"),
        col("name"),
        col("type"),
        col("title"),
        col("shortName"),
        col("startDate"),
        col("endDate"),
        col("units"))
//      cDF.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/analytics_test/cDF_explode")

    val pub_consDF = spark.read.json(pub_consF).withColumn("questionType", when(col("is_open") === "true", "open").otherwise("closed"))
      .select(col("public_consultation_id").as("pub_consId"),
        col("questionType"))
    //For test purpose
//    val uta = spark.read.json(user_total_answerF)
//      .select("questions_answers_list", explode(col("questions_answers_list")))
//
//    uta.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/analytics_test/uta_explode")

    val uta_join_c_df = spark.read.json(user_total_answerF)
      .join(broadcast(cDF), col("displayName") === col("public_consultation_id"))
      .select(explode(col("questions_answers_list").as("questions_answers_list")),
        col("displayName"),
        col("user_info"),
        col("type"))

//      uta_join_c_df.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/analytics_test/uta_join_cDF_select")

     val q_totA_join_pub_consDF = spark.read.json(q_tot_answerF).
       select(col("public_consultation_id").as("pub_consId_q"),
         col("total_answers"))
      .join(pub_consDF, col("pub_consId") === col("pub_consId_q"))
//      q_totA_join_pub_consDF.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/analytics_test/q_totA_join_consDF")
    import spark.implicits._
    val countries_schema = StructType(Seq(
      StructField("geometry", StringType, true), StructField("country", StringType, true)
    ))

    val final_DF = uta_join_c_df.join(q_totA_join_pub_consDF, col("displayName") === col("pub_consId_q"))
////    final_DF.show(1)
      .withColumn("countries", from_json($"user_info.countries", countries_schema))
      .withColumn("kind", when(col("type") === "OPC_LAUNCHED", "EUSurvey").otherwise("BRP"))
      .withColumn("openQueStats",
        struct(
          struct(
            col("questions_answers_list.main_sentiment_en.type").as("finalSentiment")
            , col("questions_answers_list.main_sentiment_en.score").as("sentimentScore")
          ).as("sentiment"),
          col("total_answers.main_sentences_edmundson_classical").as("mainSentences"),
          struct(
            col("total_answers.main_keywords_en.lemma_count").as("rank")
            , col("total_answers.main_keywords_en.lemmatized").as("text")
          ).as("keyPhrase"),
            struct(
            col("total_answers.main_entities_en.score").as("score"),
            col("total_answers.main_entities_en.type").as("type"),
            col("total_answers.main_entities_en.text").as("text")
          ).as("mainEntities")
        )
        )
      .withColumn("author",
        struct(col("owners").as("name")))
      .select(
        col("consultationId"),
        col("questions_answers_list.question_number").as("questionId"),
        col("questionType"),
        col("displayName").as("alias"),
        col("title"),
        col("shortName"),
        col("displayName"),
        col("type"),
        col("open_answers_language").as("language"),
        col("user_info.ind_type").as("userType"),
        col("user_info.companySize").as("companySize"),
        col("startDate"),
        col("endDate"),
        col("kind"),
        col("units"),
        col("openQueStats"),
        col("author")
      ).drop("questions_answers_list")
//    final_DF.printSchema()
//        final_DF.show()
    final_DF.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/analytics/final_1")

  }
}
