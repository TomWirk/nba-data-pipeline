import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.nio.file.{FileSystems, Files, Paths, StandardCopyOption}
import scala.collection.JavaConverters._



object Transform {

val spark = SparkSession.builder()
      .appName("NBA")
      .master("local[*]")
      .getOrCreate()
//(1,14,17,24)
val schema_games = StructType(Array(
    StructField("id",IntegerType,true),
    StructField("home_team",StructType(Array(
      StructField("id",StringType,true),
      StructField("full_name",StringType,true)))),
    StructField("home_team_score", StringType, true),
    StructField("visitor_team",StructType(Array(
      StructField("id",StringType,true),
      StructField("full_name",StringType,true)))),
    StructField("visitor_team_score", StringType, true)
  ))

  def main(args: Array[String]): Unit = {
    val df_teams = spark.read
    			.option("header", "true")
    			.schema(schema_games)
    			.json("games/games_data.json")
    			.withColumn("game_id",col("id").cast(IntegerType))
    			.withColumn("home_team_id",col("home_team.id").cast(IntegerType))
    			.withColumn("home_team_name",col("home_team.full_name").cast(StringType))
    			.withColumn("visitor_team_id",col("visitor_team.id").cast(IntegerType))
    			.withColumn("visitor_team_name",col("visitor_team.full_name").cast(StringType))
    			.drop("id")
    			.drop("home_team")
    			.drop("visitor_team")

    val targetTeams = Seq(1, 14, 17, 24)

	val home = df_teams
	.filter(col("home_team_id").isin(targetTeams: _*))
	.select(
		col("game_id"),
		col("home_team_id").alias("team_id"),
		col("home_team_name").alias("team_name"),
		col("home_team_score").alias("team_score")
	)
	.distinct()

	val visitor = df_teams
	.filter(col("visitor_team_id").isin(targetTeams: _*))
	.select(
		col("game_id"),
		col("visitor_team_id").alias("team_id"),
		col("visitor_team_name").alias("team_name"),
		col("visitor_team_score").alias("team_score")
	)
	.distinct()

	val result_games = home.union(visitor).distinct()

    
    //Sur les données des matchs : 
    result_games.show()
    
    val schema_stats = StructType(Array(
    StructField("id",IntegerType,true),
    StructField("game",StructType(Array(
      StructField("id",IntegerType,true)))),
    StructField("team",StructType(Array(
      StructField("id",IntegerType,true)))),
    StructField("player",StructType(Array(
      StructField("id",IntegerType,true),
      StructField("last_name",StringType,true),
      StructField("first_name",StringType,true)))),
    StructField("pts", IntegerType, true),
    StructField("reb", IntegerType, true),
    StructField("ast", IntegerType, true),
    StructField("blk", IntegerType, true)
  ))
    
    val df_stats = spark.read
    			.option("header", "true")
    			.schema(schema_stats)
    			.json("stats/stats_data.json")
    			.withColumn("stat_id",col("id").cast(IntegerType))
    			.withColumn("game_id",col("game.id").cast(IntegerType))
    			.withColumn("team_id",col("team.id").cast(IntegerType))
    			.withColumn("player_id",col("player.id").cast(IntegerType))
    			.withColumn("player_last_name",col("player.last_name").cast(StringType))
     			.withColumn("player_first_name",col("player.first_name").cast(StringType))
    			.drop("id")
    			.drop("game")
    			.drop("team")
    			.drop("player")
    			
	val teams_stats = df_stats
	.filter(col("team_id").isin(targetTeams: _*))
	.groupBy("game_id", "team_id")
	.agg(
		sum("pts").alias("pts_tot"),
		sum("reb").alias("pts_reb"),
		sum("ast").alias("pts_ast"),
		sum("blk").alias("pts_blk")
	)
	.distinct()

    
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
    
	val top_pts = players_stats
	.filter(col("pts_rank") === 1)
	.withColumn("best_at_pts", concat_ws(" ", col("player_first_name"), col("player_last_name")))
	.select("game_id", "team_id", "best_at_pts")

	val top_reb = players_stats
	.filter(col("reb_rank") === 1)
	.withColumn("best_at_reb", concat_ws(" ", col("player_first_name"), col("player_last_name")))
	.select("game_id", "team_id", "best_at_reb")

	val top_ast = players_stats
	.filter(col("ast_rank") === 1)
	.withColumn("best_at_ast", concat_ws(" ", col("player_first_name"), col("player_last_name")))
	.select("game_id", "team_id", "best_at_ast")

	val top_blk = players_stats
	.filter(col("blk_rank") === 1)
	.withColumn("best_at_blk", concat_ws(" ", col("player_first_name"), col("player_last_name")))
	.select("game_id", "team_id", "best_at_blk")

	val result_stats = teams_stats
	.join(top_pts, Seq("game_id", "team_id"))
	.join(top_reb, Seq("game_id", "team_id"))
	.join(top_ast, Seq("game_id", "team_id"))
	.join(top_blk, Seq("game_id", "team_id"))
	.select(
		col("*"),                     // toutes les colonnes de teams_stats
		col("best_at_pts"),
		col("best_at_reb"),
		col("best_at_ast"),
		col("best_at_blk")
	)

    //Sur les données des statistiques :     
    result_stats.show()
    
    val df_merge = result_games.join(result_stats,result_stats("game_id")===result_games("game_id") && result_stats("team_id")===result_games("team_id"),"inner").select(
    result_games("game_id"),
    result_games("team_id"),
    result_games("team_name"),
    result_stats("pts_tot"),
    result_stats("pts_reb"),
    result_stats("pts_ast"),
    result_stats("pts_blk"),
    result_stats("best_at_pts"),
    result_stats("best_at_reb"),
    result_stats("best_at_ast"),
    result_stats("best_at_blk"))
        
    df_merge.show()
    //df_merge.printSchema()
    
    //df_merge.write.option("header", "true").csv("csv/final.csv")
    df_merge.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",";").mode("overwrite").csv("csv")
    
    Files.deleteIfExists(Paths.get("csv/_SUCCESS"))
    Files.deleteIfExists(Paths.get("csv/._SUCCESS.crc"))
    
    val dir = FileSystems.getDefault.getPath("csv")
    val temp_name = Files.list(dir).iterator().asScala.toList(0).toString.replace(".csv.crc",".csv").replace("csv/.","csv/").replace("csv/","")
    
    Files.move(Paths.get("csv/"+temp_name), Paths.get("csv/final.csv"), StandardCopyOption.REPLACE_EXISTING)
    
    /*
    val test = spark.read
                .option("inferSchema","true")
                .option("delimiter",";")
                .option("header","true")
                .csv("csv/final.csv")
                
    test.show()
    test.printSchema()
    */
    spark.stop()
  }
}
