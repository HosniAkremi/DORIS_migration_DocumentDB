package com.everis.doris

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object feedbacks {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder.
      master("local").
      appName("feedbacks")
      .config("spark.driver.maxResultSize", "12g")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

//    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "300")
//    spark.sqlContext.setConf("spark.default.parallelism", "300")

    // program paths
    val coresF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/cores.json"
    val user_total_answerF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/user_total_answers.json"
    val pub_consF = "/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/public_consultations.json"

    //Unit Test paths
//    val coresF ="/Users/hosniakremi/Desktop/test_consult/cores"
//    val user_total_answerF = "/Users/hosniakremi/Desktop/test_consult/user_total_answers"
//    val pub_consF= "/Users/hosniakremi/Desktop/test_consult/public_consultations"

    val c1_df = spark.read.json(coresF)
    .withColumn("order", explode(col("sets.order")))
      .select(col("displayName").as("joinId"),
      explode(col("links")).as("link"),
      col("name").as("consultationId"),
      col("type").as("type"),
      col("questions_metadata"),
        col("order"))
      .withColumn("linkedQuestionId", col("link.parentId"))
        .drop("link")
        .drop("sets")

//    c1_df //Has consultations having links

    val c2_df = spark.read.json(coresF)
      .filter(size(col("links")) === lit(0))
      .withColumn("order", explode(col("sets.order")))
      .select(col("displayName").as("joinId"),
      col("name").as("consultationId"),
      col("type").as("type"),
      col("questions_metadata"),
        col("order"))
        .withColumn("linkedQuestionId", lit(null).cast(LongType))


    val c_unionDF =  c1_df.union(c2_df)
   c_unionDF.filter(col("linkedQuestionId").isNotNull).show(false)
//    c_unionDF.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/feedbacks_test/union")

    val pub_consDF = spark.read.json(pub_consF).withColumn("questionType", when(col("is_open") === "true", "open").otherwise("closed"))
      .select(col ("public_consultation_id").as("pub_consId"),
        col("questionType"))

    import spark.implicits._
    val countries_schema = StructType(Seq(
      StructField("geometry", StringType, true), StructField("country", StringType, true)
    ))
    //for test
//    val uta = spark.read.option("numPartitions", 200).json(user_total_answerF).persist(StorageLevel.MEMORY_ONLY)
//    val uta = spark.read.option("inferSchema", true).json(user_total_answerF).unpersist()

    val uta_join_c_df = spark.read.json(user_total_answerF)
//    uta_join_c_df.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/feedbacks_test/uta")
      .join(c_unionDF, col("joinId") === col("public_consultation_id"))
//    for test
//    uta_join_c_df.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/feedbacks_test/uta_join_union_addedJoinId")

    //For test
//     val final_join_union = spark.read.json("/Users/hosniakremi/Desktop/Doris+_migration_data/feedbacks_test/uta_join_union")
//       .join((broadcast(pub_consDF)), col("pub_consId") === col("joinId"))
//         final_join_union.write.json("/Users/hosniakremi/Desktop/Doris+_migration_data/feedbacks_test/final_join_union")

      val uta_join_pub_consDF = uta_join_c_df
      .join(broadcast(pub_consDF), col("pub_consId") === col("joinId"))
      .withColumn("questions_answers_list", explode(col("questions_answers_list")))
      .withColumn("id", when(col("questions_metadata.caseId") === col("questions_answers_list.question_number"), col("questions_answers_list.answer_text")))
      .withColumn("user",
        struct(
          col("id"),
          concat(col("user_info.name"), lit(" ") ,col("user_info.surname")).as("name"),
          col("user_info.ind_type").as("type"),
          col("user_info.companySize").as("companySize"))
      )
      .withColumn("attachments",
        struct(col("questions_answers_list.nameAttachment").as("name"),
          col("questions_answers_list.translated_answer").as("englishText"),
          col("questions_answers_list.source_language").as("language"),
          col("questions_answers_list.answer_text").as("originalText"),
          concat(lit("attachments/")
            , col("public_consultation_id")
          , lit("/999999/UploadedFiles/no-feedback-id/")
          , col("questions_answers_list.nameAttachment")
        ).as("originalFilePath"),
          concat(lit("attachments/")
            , col("public_consultation_id")
          ,  lit("/999999/ExtractedFiles/no-feedback-id/")
          ,  col("questions_answers_list.nameAttachment")
          ).as("extractedFilePath"),
          concat(lit("attachments/")
          , col("public_consultation_id")
          , lit("/999999/TranslatedFiles/no-feedback-id/")
          , col("questions_answers_list.nameAttachment")
        ).as("englishTextFilePath")
       )
      )
      .withColumn("countries", from_json($"user_info.countries", countries_schema))
      .withColumn("kind", when(col("type") === "OPC_LAUNCHED", "EUSurvey").otherwise("BRP"))
      .select(
//        For Test purpose
//        col("joinId"),
        col("consultationId"),
        col("_id.$oid").as("feedbackId"),
        col("questions_answers_list.question_number").as("questionId"),
        col("linkedQuestionId"),
        col("order").as("groupId"),
        col("questions_answers_list.question_number").as("answerIds"),
        col("questionType"),
        col("questions_answers_list.answer_text").as("answer_text"),
        col("open_answers_language").as("language"),
        col("questions_answers_list.translated_answer").as("englishText"),
        col("countries.country").as("country"),
        col("questions_answers_list.group").as("poolId"),
        col("kind"),
        col("user"),
        col("attachments")
    )
//    uta_join_c_df.printSchema()
//      uta_join_c_df.show()

    val finalDF_partition = uta_join_pub_consDF.repartition(1)
    finalDF_partition.show()
//    finalDF_partition.write.save("/Users/hosniakremi/Desktop/Doris+_migration_data/feedbacks/")

//    For test purpose
//    uta_join_c_df.filter(col("joinId") === "19839_Ares(2017)1900557").show(3, false)

//    println(uta_join_c_df.filter(col("linkedQuestionId") === null).count())

//    uta_join_c_df.write.json("/Users/hosniakremi/Desktop/feedbacks/")

    //Defining Test Consultation
//      val validationCons = spark.read.json("/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/cores.json")
//        .filter(col("displayName") === "28975_COM(2017)278")
//      val validationFeedbacks = spark.read.json("/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/user_total_answers.json")
//      .filter(col("public_consultation_id") === "28975_COM(2017)278")
//      val validationPubCons =  spark.read.json("/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/public_consultations.json")
//      .filter(col("public_consultation_id") === "28975_COM(2017)278")
//      val validationqTotalAnswers = spark.read.json("/Users/hosniakremi/Documents/Doris+/CollectionsPRO_new_PRO_DATA/question_total_answers.json")
//      .filter(col("public_consultation_id") === "28975_COM(2017)278")

//      validationCons.write.json("/Users/hosniakremi/Desktop/test_consult/cores")
//      validationFeedbacks.write.json("/Users/hosniakremi/Desktop/test_consult/user_total_answers")
//      validationPubCons.write.json("/Users/hosniakremi/Desktop/test_consult/public_consultations")
//      validationqTotalAnswers.write.json("/Users/hosniakremi/Desktop/test_consult/question_total_answers")
  }
}
