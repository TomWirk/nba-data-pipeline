Ouvrir un terminal linux à cet emplacement et lancer la commande sbt

Lancer la commande run, vous aurez le choix entre l'éxécution de 2 fichiers d'éxecution:
	
	_ Extract.scala
	_ Transform.scala
	
Extract.scala permet d'extraire les données de la NBA et de générer 2 fichier JSON : 

	_ games/games_data.json
	_ stats/stats_data.json

Transform.scala créer 2 dataFrames à partir des fichier json générés précédement qui seront affiché lors de l'éxécution:

	_ result_games (avec le nombre de points marqués par l'équipe, l'id du match, le nom et l'id de l'équipe)
	_ result_stats (avec l'id du match, l'id de l'équipe, le nombre de points marqués (pts), le nombre de rebonds (reb), le nombre de passe décisive (ast) et le nombre de blocks (blk), ainsi que les joueurs avec les meilleurs résultats dans ces 4 domaines par équipe et par match)
	
Un troisième DataFrame "df_merge" issus de la fusion des 2 précédents sera créé et sauvegardé dans le fichier "csv/final.csv"
