import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Transform {
  def main(args: Array[String]): Unit = {
    
    val S3Path = "s3://nba-data-pipeline/"

    val spark = SparkSession.builder()
      .appName("NBA")
      .getOrCreate()

    val schema_games = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("home_team", StructType(Array(
        StructField("id", StringType, true),
        StructField("full_name", StringType, true)
      ))),
      StructField("home_team_score", StringType, true),
      StructField("visitor_team", StructType(Array(
        StructField("id", StringType, true),
        StructField("full_name", StringType, true)
      ))),
      StructField("visitor_team_score", StringType, true)
    ))

    val df_teams = spark.read
      .schema(schema_games)
      .json(S3Path + "bronze/sample/games_data.json")
      .withColumn("game_id", col("id").cast(IntegerType))
      .withColumn("home_team_id", col("home_team.id").cast(IntegerType))
      .withColumn("home_team_name", col("home_team.full_name").cast(StringType))
      .withColumn("visitor_team_id", col("visitor_team.id").cast(IntegerType))
      .withColumn("visitor_team_name", col("visitor_team.full_name").cast(StringType))
      .drop("id", "home_team", "visitor_team")

    val targetTeams = Seq(1, 14, 17, 24)

    val home = df_teams
      .filter(col("home_team_id").isin(targetTeams: _*))
      .select(
        col("game_id"),
        col("home_team_id").alias("team_id"),
        col("home_team_name").alias("team_name"),
        col("home_team_score").alias("team_score")
      ).distinct()

    val visitor = df_teams
      .filter(col("visitor_team_id").isin(targetTeams: _*))
      .select(
        col("game_id"),
        col("visitor_team_id").alias("team_id"),
        col("visitor_team_name").alias("team_name"),
        col("visitor_team_score").alias("team_score")
      ).distinct()

    val result_games = home.union(visitor).distinct()

    val schema_stats = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("game", StructType(Array(StructField("id", IntegerType, true)))),
      StructField("team", StructType(Array(StructField("id", IntegerType, true)))),
      StructField("player", StructType(Array(
        StructField("id", IntegerType, true),
        StructField("last_name", StringType, true),
        StructField("first_name", StringType, true)
      ))),
      StructField("pts", IntegerType, true),
      StructField("reb", IntegerType, true),
      StructField("ast", IntegerType, true),
      StructField("blk", IntegerType, true)
    ))

    val df_stats = spark.read
      .schema(schema_stats)
      .json(S3Path + "bronze/sample/stats_data.json")
      .withColumn("stat_id", col("id").cast(IntegerType))
      .withColumn("game_id", col("game.id").cast(IntegerType))
      .withColumn("team_id", col("team.id").cast(IntegerType))
      .withColumn("player_id", col("player.id").cast(IntegerType))
      .withColumn("player_last_name", col("player.last_name").cast(StringType))
      .withColumn("player_first_name", col("player.first_name").cast(StringType))
      .drop("id", "game", "team", "player")

    val teams_stats = df_stats
      .filter(col("team_id").isin(targetTeams: _*))
      .groupBy("game_id", "team_id")
      .agg(
        sum("pts").alias("pts_tot"),
        sum("reb").alias("pts_reb"),
        sum("ast").alias("pts_ast"),
        sum("blk").alias("pts_blk")
      ).distinct()

    val windowByTeamGamePts = Window.partitionBy("game_id", "team_id").orderBy(col("pts").desc, col("player_last_name"))
    val windowByTeamGameReb = Window.partitionBy("game_id", "team_id").orderBy(col("reb").desc, col("player_last_name"))
    val windowByTeamGameAst = Window.partitionBy("game_id", "team_id").orderBy(col("ast").desc, col("player_last_name"))
    val windowByTeamGameBlk = Window.partitionBy("game_id", "team_id").orderBy(col("blk").desc, col("player_last_name"))

    val players_stats = df_stats
      .filter(col("team_id").isin(targetTeams: _*))
      .select("game_id", "team_id", "player_id", "player_last_name", "player_first_name", "pts", "reb", "ast", "blk")
      .withColumn("pts_rank", rank().over(windowByTeamGamePts))
      .withColumn("reb_rank", rank().over(windowByTeamGameReb))
      .withColumn("ast_rank", rank().over(windowByTeamGameAst))
      .withColumn("blk_rank", rank().over(windowByTeamGameBlk))
      .distinct()

    val top_pts = players_stats.filter(col("pts_rank") === 1)
      .withColumn("best_at_pts", concat_ws(" ", col("player_first_name"), col("player_last_name")))
      .select("game_id", "team_id", "best_at_pts")

    val top_reb = players_stats.filter(col("reb_rank") === 1)
      .withColumn("best_at_reb", concat_ws(" ", col("player_first_name"), col("player_last_name")))
      .select("game_id", "team_id", "best_at_reb")

    val top_ast = players_stats.filter(col("ast_rank") === 1)
      .withColumn("best_at_ast", concat_ws(" ", col("player_first_name"), col("player_last_name")))
      .select("game_id", "team_id", "best_at_ast")

    val top_blk = players_stats.filter(col("blk_rank") === 1)
      .withColumn("best_at_blk", concat_ws(" ", col("player_first_name"), col("player_last_name")))
      .select("game_id", "team_id", "best_at_blk")

    val result_stats = teams_stats
      .join(top_pts, Seq("game_id", "team_id"))
      .join(top_reb, Seq("game_id", "team_id"))
      .join(top_ast, Seq("game_id", "team_id"))
      .join(top_blk, Seq("game_id", "team_id"))

    val df_merge = result_games.join(
      result_stats,
      Seq("game_id", "team_id"),
      "inner"
    ).select(
      col("game_id"), col("team_id"), col("team_name"),
      col("pts_tot"), col("pts_reb"), col("pts_ast"), col("pts_blk"),
      col("best_at_pts"), col("best_at_reb"), col("best_at_ast"), col("best_at_blk")
    )

    // Écriture directe sur S3 (CSV natif)
    df_merge
      .coalesce(1) // OK pour démo, à éviter en prod
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(S3Path+"silver/sample")

    spark.stop()
  }
}

