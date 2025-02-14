import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column


object Main {
  def main(args: Array[String]): Unit = {

    val connPostgres: Connexion = new Connexion("Postgres")
    val connOracle: Connexion = new Connexion("Oracle")

    //Appel du dialect
    val dialect = new OracleDialect
    JdbcDialects.registerDialect(dialect)

    //Initialisation de Spark
    val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()


    /**************** CSV ****************/

    val fichierCSV = "yelp_academic_dataset_tip.csv"

    //Chargement du fichier CSV
    var tips = spark.read.format("csv").option("header", "true").load(fichierCSV)

    //Extraction des user_id
    var tip = tips.select("business_id", "compliment_count", "date", "user_id")

    /*******************************************/



    /**************** JSON ****************/

    val businessFile = "yelp_academic_dataset_business.json"
    val checkinFile = "yelp_academic_dataset_checkin.json"

    // Chargement des fichier JSON
    var businesses = spark.read.json(businessFile).cache()
    var checkin = spark.read.json(checkinFile).cache()

    /*******************************************/


    /**************** POSTGRES ****************/
    val reviews = spark.read.jdbc(connPostgres.url, "yelp.review", connPostgres.connectionProperties)
    val users = spark.read.jdbc(connPostgres.url, "yelp.user", connPostgres.connectionProperties)
    val elites = spark.read.jdbc(connPostgres.url, "yelp.elite", connPostgres.connectionProperties)
    val friends = spark.read.jdbc(connPostgres.url, "yelp.friend", connPostgres.connectionProperties)

    val review = reviews.select("business_id", "date", "funny", "stars", "cool", "useful", "user_id")
    val user = users.select("user_id", "name", "yelping_since", "fans")
    val elite = elites.select("user_id", "year")
    val friend = friends.select("user_id")

    /*******************************************/



    /**************** DATA MART BUSINNES ****************/
    // Jointure tip et business
    var jointureBusinessTip = businesses
      .join(tip, businesses("business_id") === tip("business_id"), "inner")
      .select(
        businesses("business_id"),
        concat(businesses("city"), lit(" - "), businesses("postal_code")).alias("city"),
        tip("compliment_count"),
        date_format(tip("date"), "yyyy-MM-dd").alias("date")
      )

    // Aggregation Tip
    var aggregationTip = jointureBusinessTip.groupBy("business_id", "city", "date").agg(
      sum("compliment_count").as("nbCompliment"),
      avg("compliment_count").as("moyenneCompliment"),
      count("compliment_count").as("nbTip")
    ).withColumn("nbCommentaire", lit(null))
    .withColumn("moyenneStars", lit(null))
    .withColumn("nbVoteFunny", lit(null))
    .withColumn("nbVoteCool", lit(null))
    .withColumn("nbVoteUseful", lit(null))
    .withColumn("moyenneVoteFunny", lit(null))
    .withColumn("moyenneVoteCool", lit(null))
    .withColumn("moyenneVoteUseful", lit(null))
    .withColumn("nbVisite", lit(null))



    // Jointure review et business
    var jointureBusinessReview = businesses
      .join(review, businesses("business_id") === review("business_id"), "inner")
      .select(
        businesses("business_id"),
        concat(businesses("city"), lit(" - "), businesses("postal_code")).alias("city"),
        review("useful"),
        review("funny"),
        review("cool"),
        review("stars"),
        review("date")
      )

    // Aggregation Review
    var aggregationReview = jointureBusinessReview.groupBy("business_id", "city", "date").agg(
      count("stars").as("nbCommentaire"),
      avg("stars").as("moyenneStars"),
      sum("funny").as("nbVoteFunny"),
      sum("cool").as("nbVoteCool"),
      sum("useful").as("nbVoteUseful"),
      avg("funny").as("moyenneVoteFunny"),
      avg("cool").as("moyenneVoteCool"),
      avg("useful").as("moyenneVoteUseful")
    ).withColumn("nbCompliment", lit(null))
    .withColumn("moyenneCompliment", lit(null))
    .withColumn("nbTip", lit(null))
    .withColumn("nbVisite", lit(null))


    // Extraction des données sur les Checkin
    val recupCheckin = checkin.withColumn("date", explode(org.apache.spark.sql.functions.split(col("date"), ",")))

    val debutCheckin = recupCheckin.select(col("business_id"), col("date"))

    // Jointure checkin et business
    var jointureBusinessCheckin = businesses
      .join(debutCheckin, businesses("business_id") === debutCheckin("business_id"), "inner")
      .select(
        businesses("business_id"),
        concat(businesses("city"), lit(" - "), businesses("postal_code")).alias("city"),
        date_format(debutCheckin("date"), "yyyy-MM-dd").alias("date")
      )

    // Aggregation Checkin
    var aggregationCheckin = jointureBusinessCheckin.groupBy("business_id", "city", "date").agg(
      count("date").as("nbVisite")
    ).withColumn("nbCommentaire", lit(null))
    .withColumn("moyenneStars", lit(null))
    .withColumn("nbVoteFunny", lit(null))
    .withColumn("nbVoteCool", lit(null))
    .withColumn("nbVoteUseful", lit(null))
    .withColumn("moyenneVoteFunny", lit(null))
    .withColumn("moyenneVoteCool", lit(null))
    .withColumn("moyenneVoteUseful", lit(null))
    .withColumn("nbCompliment", lit(null))
    .withColumn("moyenneCompliment", lit(null))
    .withColumn("nbTip", lit(null))


    //Union entre aggregationReview et aggregationTip
    var unionTipAndReview = aggregationReview.select(
      col("business_id"),
      col("city"),
      col("date"),
      col("nbVisite"),
      col("nbCommentaire"),
      col("moyenneStars"),
      col("nbVoteFunny"),
      col("nbVoteCool"),
      col("nbVoteUseful"),
      col("moyenneVoteFunny"),
      col("moyenneVoteCool"),
      col("moyenneVoteUseful"),
      col("nbCompliment"),
      col("moyenneCompliment"),
      col("nbTip")
    ).union(aggregationTip.select(
      col("business_id"),
      col("city"),
      col("date"),
      col("nbVisite"),
      col("nbCommentaire"),
      col("moyenneStars"),
      col("nbVoteFunny"),
      col("nbVoteCool"),
      col("nbVoteUseful"),
      col("moyenneVoteFunny"),
      col("moyenneVoteCool"),
      col("moyenneVoteUseful"),
      col("nbCompliment"),
      col("moyenneCompliment"),
      col("nbTip")
    ))



    //Union entre unionTipAndReview et aggregationCheckin
    var unionTipReviewAndCheckin = unionTipAndReview.select(
      col("business_id"),
      col("city"),
      col("date"),
      col("nbVisite"),
      col("nbCommentaire"),
      col("moyenneStars"),
      col("nbVoteFunny"),
      col("nbVoteCool"),
      col("nbVoteUseful"),
      col("moyenneVoteFunny"),
      col("moyenneVoteCool"),
      col("moyenneVoteUseful"),
      col("nbCompliment"),
      col("moyenneCompliment"),
      col("nbTip")
    ).union(aggregationCheckin.select(
      col("business_id"),
      col("city"),
      col("date"),
      col("nbVisite"),
      col("nbCommentaire"),
      col("moyenneStars"),
      col("nbVoteFunny"),
      col("nbVoteCool"),
      col("nbVoteUseful"),
      col("moyenneVoteFunny"),
      col("moyenneVoteCool"),
      col("moyenneVoteUseful"),
      col("nbCompliment"),
      col("moyenneCompliment"),
      col("nbTip")
    ))


    //Regroupement des données pour supprimer les doublons sur les dimensions
    var tipReviewAndCheckinSansDoublon = unionTipReviewAndCheckin.groupBy("business_id", "city", "date").agg(
      sum("nbVisite").as("nbVisite"),
      sum("nbCommentaire").as("nbCommentaire"),
      avg("moyenneStars").as("moyenneStars"),
      sum("nbVoteFunny").as("nbVoteFunny"),
      sum("nbVoteCool").as("nbVoteCool"),
      sum("nbVoteUseful").as("nbVoteUseful"),
      avg("moyenneVoteFunny").as("moyenneVoteFunny"),
      avg("moyenneVoteCool").as("moyenneVoteCool"),
      avg("moyenneVoteUseful").as("moyenneVoteUseful"),
      sum("nbCompliment").as("nbCompliment"),
      sum("moyenneCompliment").as("moyenneCompliment"),
      sum("nbTip").as("nbTip")
    )


    // Création de la dimension Time et récupération du mois, de la semaine et de l'année d'une date
    var debutDimensionTime = tipReviewAndCheckinSansDoublon.select(
      col("date"),
      year(col("date")).cast("string").as("year"),
      month(col("date")).cast("string").as("month"),
      weekofyear(col("date")).cast("string").as("week")
    ).distinct()
    .withColumn("idTime", monotonically_increasing_id())


    // Jointure pour l'id du time
    var jointureForIdTime = tipReviewAndCheckinSansDoublon
      .join(debutDimensionTime, tipReviewAndCheckinSansDoublon("date") === debutDimensionTime("date"), "inner")
      .select(
        debutDimensionTime("idTime"),
        tipReviewAndCheckinSansDoublon("business_id"),
        tipReviewAndCheckinSansDoublon("city"),
        tipReviewAndCheckinSansDoublon("nbVisite"),
        tipReviewAndCheckinSansDoublon("nbCommentaire"),
        tipReviewAndCheckinSansDoublon("moyenneStars"),
        tipReviewAndCheckinSansDoublon("nbVoteFunny"),
        tipReviewAndCheckinSansDoublon("nbVoteCool"),
        tipReviewAndCheckinSansDoublon("nbVoteUseful"),
        tipReviewAndCheckinSansDoublon("moyenneVoteFunny"),
        tipReviewAndCheckinSansDoublon("moyenneVoteCool"),
        tipReviewAndCheckinSansDoublon("moyenneVoteUseful"),
        tipReviewAndCheckinSansDoublon("nbCompliment"),
        tipReviewAndCheckinSansDoublon("moyenneCompliment"),
        tipReviewAndCheckinSansDoublon("nbTip")
      )

    var dimensionTime = debutDimensionTime.select(
      col("idTime"),
      to_date(col("date"), "yyyy-MM-dd").as("date"),
      col("year"),
      col("month"),
      col("week")
    )


    // Extraction des données géographiques
    var exctractionGeo = businesses.select(concat(businesses("city"), lit(" - "), businesses("postal_code")).alias("cityConcat"),col("city"), col("state"), col("postal_code").cast("int").as("cp")).distinct()
      .withColumn("idGeo", monotonically_increasing_id())


    // Jointure pour l'id du geo
    var factTableBusiness = jointureForIdTime
      .join(exctractionGeo, jointureForIdTime("city") === exctractionGeo("cityConcat"), "inner")
      .select(
        exctractionGeo("idGeo"),
        jointureForIdTime("idTime"),
        jointureForIdTime("business_id"),
        jointureForIdTime("nbVisite"),
        jointureForIdTime("nbCommentaire"),
        jointureForIdTime("moyenneStars"),
        jointureForIdTime("nbVoteFunny"),
        jointureForIdTime("nbVoteCool"),
        jointureForIdTime("nbVoteUseful"),
        jointureForIdTime("moyenneVoteFunny"),
        jointureForIdTime("moyenneVoteCool"),
        jointureForIdTime("moyenneVoteUseful"),
        jointureForIdTime("nbCompliment"),
        jointureForIdTime("moyenneCompliment"),
        jointureForIdTime("nbTip")
      )

      var dimensionGeo = exctractionGeo.select(col("idGeo"), col("city"), col("state"), col("cp"))



    //Extraction des infos d'ouverture et créer une table de 5 colonnes : "idHoraire", "business_id", "type_jour", "heureOuverture" et "heureFermeture"
    var dimensionHoraireOuverture = businesses.selectExpr(
        "monotonically_increasing_id() as idHoraire",
        "business_id",
        "stack(7, " +
          s"'Monday', hours.Monday, " +
          s"'Tuesday', hours.Tuesday, " +
          s"'Wednesday', hours.Wednesday, " +
          s"'Thursday', hours.Thursday, " +
          s"'Friday', hours.Friday, " +
          s"'Saturday', hours.Saturday, " +
          s"'Sunday', hours.Sunday" +
          ") as (type_jour, heures)",
        "substring_index(heures, '-', 1) as heureOuverture",
        "substring_index(heures, '-', -1) as heureFermeture"
      ).drop("heures")


    // Extraction des données sur les Categories de commerces
    var recupCategorie = businesses.withColumn("categories", explode(org.apache.spark.sql.functions.split(col("categories"), ",")))

    // Exctraction des 2 colonnes nécessaire
    var debutDimensionCategories = recupCategorie.select(col("business_id"), col("categories").as("categorie"))

    //Récupération des 20 catégories les plus utilisées
    var categorieFamous = debutDimensionCategories.groupBy("categorie").count().orderBy(col("count").desc).limit(20).withColumn("idCategorie", monotonically_increasing_id())

    //Création de la dimension Categorie
    var dimensionCategories = debutDimensionCategories
      .join(categorieFamous, debutDimensionCategories("categorie") === categorieFamous("categorie"), "inner")
      .select(
        categorieFamous("idCategorie"),
        debutDimensionCategories("business_id"),
        categorieFamous("categorie")
      )

    //création de la dimension commerce
    var dimensionCommerce = businesses.select(col("business_id"), col("name"))

    /*******************************************/


    /**************** DATA MART USER ****************/

    // Compte le nombre de elite par personne
    var nbElite = elite.groupBy("user_id").count()

    // Compte le nombre d'amis par personne
    var nbFriend = friend.groupBy("user_id").count()

    
    // Jointure elite et friend
    var jointureEliteAndFriend = nbElite
      .join(nbFriend, nbElite("user_id") === nbFriend("user_id"), "inner")
      .select(
        nbFriend("user_id"),
        nbFriend("count").as("nbAmis"),
        nbElite("count").as("nbElite")
      )
      

    // Jointure pour récuperer nombre fan d'un utilisateur
    var jointureEliteFriendAndUser = jointureEliteAndFriend
      .join(user, user("user_id") === jointureEliteAndFriend("user_id"), "inner")
      .select(
        jointureEliteAndFriend("user_id"),
        jointureEliteAndFriend("nbAmis"),
        jointureEliteAndFriend("nbElite"),
        user("fans").as("nbFans")
      ).withColumn("nbCommentaire", lit(null))
    .withColumn("moyenneStars", lit(null))
    .withColumn("nbVoteFunny", lit(null))
    .withColumn("nbVoteCool", lit(null))
    .withColumn("nbVoteUseful", lit(null))
    .withColumn("moyenneVoteFunny", lit(null))
    .withColumn("moyenneVoteCool", lit(null))
    .withColumn("moyenneVoteUseful", lit(null))
    .withColumn("idTime", lit(null))

    // Aggregation Review
    var aggregationReviewForUser = review.groupBy("user_id", "date").agg(
      count("stars").as("nbCommentaire"),
      avg("stars").as("moyenneStars"),
      sum("funny").as("nbVoteFunny"),
      sum("cool").as("nbVoteCool"),
      sum("useful").as("nbVoteUseful"),
      avg("funny").as("moyenneVoteFunny"),
      avg("cool").as("moyenneVoteCool"),
      avg("useful").as("moyenneVoteUseful")
    )

    // Création de la dimension Time et récupération du mois, de la semaine et de l'année d'une date
    var debutDimensionTimeUser = aggregationReviewForUser.select(
      col("date"),
      year(col("date")).cast("string").as("year"),
      month(col("date")).cast("string").as("month"),
      weekofyear(col("date")).cast("string").as("week")
    ).distinct()
    .withColumn("idTime", monotonically_increasing_id())


    // Jointure pour l'id du time
    var reviewUser = aggregationReviewForUser
      .join(debutDimensionTimeUser, aggregationReviewForUser("date") === debutDimensionTimeUser("date"), "inner")
      .select(
        debutDimensionTimeUser("idTime"),
        aggregationReviewForUser("user_id").as("id_user"),
        aggregationReviewForUser("nbCommentaire"),
        aggregationReviewForUser("moyenneStars"),
        aggregationReviewForUser("nbVoteFunny"),
        aggregationReviewForUser("nbVoteCool"),
        aggregationReviewForUser("nbVoteUseful"),
        aggregationReviewForUser("moyenneVoteFunny"),
        aggregationReviewForUser("moyenneVoteCool"),
        aggregationReviewForUser("moyenneVoteUseful")
      ).withColumn("nbAmis", lit(null))
    .withColumn("nbElite", lit(null))
    .withColumn("nbFans", lit(null))

    //Union entre aggregationReviewForUser et jointureEliteFriendAndUser
    var unionReviewAndUser = reviewUser.select(
      col("id_user"),
      col("idTime"),
      col("nbCommentaire"),
      col("moyenneStars"),
      col("nbVoteFunny"),
      col("nbVoteCool"),
      col("nbVoteUseful"),
      col("moyenneVoteFunny"),
      col("moyenneVoteCool"),
      col("moyenneVoteUseful"),
      col("nbAmis"),
      col("nbElite"),
      col("nbFans")
    ).union(jointureEliteFriendAndUser.select(
      col("user_id").as("id_user"),
      col("idTime"),
      col("nbCommentaire"),
      col("moyenneStars"),
      col("nbVoteFunny"),
      col("nbVoteCool"),
      col("nbVoteUseful"),
      col("moyenneVoteFunny"),
      col("moyenneVoteCool"),
      col("moyenneVoteUseful"),
      col("nbAmis"),
      col("nbElite"),
      col("nbFans")
    ))



    //Regroupement des données pour supprimer les doublons sur les dimensions
    var factTableUser = unionReviewAndUser.groupBy("id_user", "idTime").agg(
      sum("nbCommentaire").as("nbCommentaire"),
      avg("moyenneStars").as("moyenneStars"),
      sum("nbVoteFunny").as("nbVoteFunny"),
      sum("nbVoteCool").as("nbVoteCool"),
      sum("nbVoteUseful").as("nbVoteUseful"),
      avg("moyenneVoteFunny").as("moyenneVoteFunny"),
      avg("moyenneVoteCool").as("moyenneVoteCool"),
      avg("moyenneVoteUseful").as("moyenneVoteUseful"),
      sum("nbAmis").as("nbAmis"),
      sum("nbElite").as("nbElite"),
      sum("nbFans").as("nbFans")
    )


    // Création de la dimension Time de user
    var dimensionTimeUser = debutDimensionTimeUser.select(
      col("idTime"),
      to_date(col("date"), "yyyy-MM-dd").as("date"),
      col("year"),
      col("month"),
      col("week")
    )

    // Création de la dimension User
    var dimensionUser = user.select(
      col("user_id").as("id_user"),
      col("name"),
      col("yelping_since").as("dateArrivee")
    )
    /*******************************************/


    /**************** ECRITURE DANS LA BDD DATA MART BUSINNESS ****************/
    // Dimension de temps
    dimensionTime.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "TIME", connOracle.connectionProperties)

    // Dimension géographique
    dimensionGeo.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "GEO", connOracle.connectionProperties)

    // Dimension Horaire Ouverture
    dimensionHoraireOuverture.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "HORAIRE_OUVERTURE", connOracle.connectionProperties)

    // Dimension Categorie
    dimensionCategories.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "CATEGORIE", connOracle.connectionProperties)

    // Dimension Commerce
    dimensionCommerce.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "BUSINESS", connOracle.connectionProperties)

    //Table de fait
    factTableBusiness.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "FACT_BUSINESS", connOracle.connectionProperties)

    /*******************************************/


    /**************** ECRITURE DANS LA BDD DATA MART USER ****************/
    // Dimension de temps
    dimensionTimeUser.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "TIME_USER", connOracle.connectionProperties)

    // Dimension utilisateur
    dimensionUser.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "DIM_USER", connOracle.connectionProperties)

    //Table de fait
    factTableUser.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "FACT_USER", connOracle.connectionProperties)

    *******************************************/

    spark.stop()
  }
}


