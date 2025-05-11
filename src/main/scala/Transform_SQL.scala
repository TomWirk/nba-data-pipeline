import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
import org.apache.spark.sql.functions.col
import java.nio.file.{FileSystems, Files, Paths, StandardCopyOption}
import scala.collection.JavaConverters._



object TransformWithSQL {

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

    df_teams.createOrReplaceTempView("GAMES")
    val result_games = spark.sql(
    	"""
    	SELECT distinct
    		game_id,
    		home_team_id as team_id,
    		home_team_name as team_name,
    		home_team_score as team_score
    	FROM GAMES
    	where home_team_id in (1,14,17,24)
    	union
    	SELECT distinct
    		game_id,
    		visitor_team_id as team_id,
    		visitor_team_name as team_name,
    		visitor_team_score as team_score
    	FROM GAMES
    	where visitor_team_id in (1,14,17,24)
    	"""
    )
    
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

    df_stats.createOrReplaceTempView("STATS")
    			
    val teams_stats = spark.sql(
    	"""
    	SELECT distinct
    		game_id,
    		team_id,
    		sum(pts) as pts_tot,
    		sum(reb) as pts_reb,
    		sum(ast) as pts_ast,
    		sum(blk) as pts_blk
    	FROM STATS
    	where team_id in (1,14,17,24)
    	group by game_id, team_id
    	
    	"""
    )
    
    
    teams_stats.createOrReplaceTempView("TEAMS_STATS")
    
    val players_stats = spark.sql(
    	"""
    	SELECT distinct
    		game_id,
    		team_id,
    		player_id,
    		player_last_name,
    		player_first_name,
    		RANK() OVER (PARTITION BY game_id,team_id ORDER BY pts DESC, player_last_name) AS pts_rank,
    		RANK() OVER (PARTITION BY game_id,team_id ORDER BY reb DESC, player_last_name) AS reb_rank,
    		RANK() OVER (PARTITION BY game_id,team_id ORDER BY ast DESC, player_last_name) AS ast_rank,
    		RANK() OVER (PARTITION BY game_id,team_id ORDER BY blk DESC, player_last_name) AS blk_rank
    	FROM STATS
    	where team_id in (1,14,17,24)
    	
    	"""
    )
    
    players_stats.createOrReplaceTempView("PLAYERS_STATS")
    
    val top_pts = spark.sql(
    	"""
    	SELECT 
    		game_id,
    		team_id,
    		CONCAT(player_first_name,' ', player_last_name) as best_at_pts
    	FROM PLAYERS_STATS
    	where pts_rank = 1
    	
    	"""
    )
    
    val top_reb = spark.sql(
    	"""
    	SELECT 
    		game_id,
    		team_id,
    		CONCAT(player_first_name,' ', player_last_name) as best_at_reb
    	FROM PLAYERS_STATS
    	where reb_rank = 1
    	
    	"""
    )
    
    val top_ast = spark.sql(
    	"""
    	SELECT 
    		game_id,
    		team_id,
    		CONCAT(player_first_name,' ', player_last_name) as best_at_ast
    	FROM PLAYERS_STATS
    	where ast_rank = 1
    	
    	"""
    ) 
    
    val top_blk = spark.sql(
    	"""
    	SELECT 
    		game_id,
    		team_id,
    		CONCAT(player_first_name,' ', player_last_name) as best_at_blk
    	FROM PLAYERS_STATS
    	where blk_rank = 1
    	
    	"""
    ) 
    
    top_pts.createOrReplaceTempView("TOP_PTS")
    top_reb.createOrReplaceTempView("TOP_REB")    
    top_ast.createOrReplaceTempView("TOP_AST")
    top_blk.createOrReplaceTempView("TOP_BLK")
    /*
    val result_stats = spark.sql(
    	"""
    	SELECT * from TEAMS_STATS,TOP_PTS,TOP_REB,TOP_AST,TOP_BLK
    	
    	"""
    )
    
    */
    val result_stats = spark.sql(
    	"""
    	SELECT t.*, p.best_at_pts, r.best_at_reb, a.best_at_ast, b.best_at_blk from TEAMS_STATS t
    	inner join TOP_PTS p
    	on p.game_id = t.game_id
    	and p.team_id = t.team_id
    	inner join TOP_REB r
    	on r.game_id = t.game_id
    	and r.team_id = t.team_id
    	inner join TOP_AST a
    	on a.game_id = t.game_id
    	and a.team_id = t.team_id
    	inner join TOP_BLK b
    	on b.game_id = t.game_id
    	and b.team_id = t.team_id
    	
    	"""
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
    
    //df_merge.createOrReplaceTempView("MERGE")
    
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
