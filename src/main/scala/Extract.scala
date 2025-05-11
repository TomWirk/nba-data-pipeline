import ujson._
import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object Extract {
  val teams_url: String = "https://api.balldontlie.io/v1/teams"
  val games_url: String = "https://api.balldontlie.io/v1/games"
  val stats_url: String = "https://api.balldontlie.io/v1/stats"
  val team_names: List[String] = List[String]("Phoenix Suns", "Atlanta Hawks", "Los Angeles Lakers", "Milwaukee Bucks")
  val api_key: String = "0d9dee05-0db1-4c65-b07e-c26b7b809454"
  val headers: Map[String, String] = Map("Authorization" -> api_key)
  val delayBetweenRequests: Int = 2000 // 2 seconds in milliseconds
  val maxRetries: Int = 5

  def extract_page(url: String): ujson.Value.Value = {
    /*
    Extract json content of a page using a http get request
    Arg:
      url: String -> the url we extract the json content from
    Return:
      ujs: usjon.Value.Value -> json content of the page
    */
    val res: requests.Response = requests.get(url, headers = headers)
    val ujs: ujson.Value.Value = ujson.read(res.text)
    return ujs
  }

  def extract_pages_data(url: String): List[ujson.Value] = {
    var pages_data: List[ujson.Value] = List[ujson.Value]()
    var cursor: ujson.Value = 0
    while (cursor != ujson.Null) {
      val charac: String = if (url.contains("?")) "&" else "?"
      val ujs: ujson.Value = extract_page(s"${url}${charac}cursor=${cursor}")
      val data: ujson.Value = ujs("data")
      
      pages_data :+= data
      
      try {
        val metadata: ujson.Value = ujs("meta")
        cursor = metadata("next_cursor")
      } catch {
        case e: Exception =>
          println(s"There isn't any page left for ${url}")
          cursor = ujson.Null
      }

      // Add delay between requests
      println(s"Waiting for $delayBetweenRequests ms before the next request...")
      Thread.sleep(delayBetweenRequests)
    }
    return pages_data
  }
  
  def extract_team_ids(
    teams_url: String,
    team_names: List[String]
  ): List[Int] = {
    /*
    Extract the selected team ids
    Args:
      teams_url: String -> the api route giving team informations
      team_names: List[String] -> the team names
    Return:
      team_ids: List[Int] -> the team ids of the selected teams
    */
    val pages_data = extract_pages_data(teams_url)
    var team_ids: List[Int] = List[Int]()
    for (page_data <- pages_data) {
      val teams_selected = page_data.arr.filter(
        team => team_names.contains(team("full_name").toString().replace("\"", "")))
      teams_selected.arr.foreach(team => team_ids :+= team("id").num.toInt)
    }
    return team_ids
  }

  def extract_game_ids(
    games_url: String
  ): List[Int] = {
    /*
    Extract the selected game ids
    Args:
      games_url: String -> the api route giving game informations
      game_names: List[String] -> the game names
    Return:
      game_ids: List[Int] -> the game ids of the selected games
    */
    val pages_data = extract_pages_data(games_url)
    Files.write(Paths.get("games/games_data.json"), pages_data.mkString.replace("][",",").getBytes(StandardCharsets.UTF_8))
    var game_ids: List[Int] = List[Int]()
    for (page_data <- pages_data) {
      page_data.arr.foreach(game => game_ids :+= game("id").num.toInt)
    }
    return game_ids
  }

  def extract_stats(
    stats_url: String
  ): Unit = {
    /*
    Extract the selected stat ids
    Args:
      stats_url: String -> the api route giving stat informations
      stat_names: List[String] -> the stat names
    Return:
      stat_ids: List[Int] -> the stat ids of the selected stats
    */
    val pages_data = extract_pages_data(stats_url)
    Files.write(Paths.get("stats/stats_data.json"), pages_data.mkString.replace("][",",").getBytes(StandardCharsets.UTF_8))
    //saveDataAsJson(pages_data, "stats/stats_data.json")
  }

  def main(args: Array[String]) {
    /*
    Extract 4 team ids from the balldontlie api
    */
    val team_ids = extract_team_ids(teams_url, team_names)
    val game_ids = extract_game_ids(games_url + "?team_ids[]=" + team_ids.mkString("&team_ids[]=") + "&seasons[]=2023")
    extract_stats(stats_url + "?game_ids[]=" + game_ids.mkString("&game_ids[]=") + "&seasons[]=2023")
    
    
  }
}
