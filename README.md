StormTwitter
============

TopKWords from twitter using Apache storm and Twitter Streaming API

This application uses Apache Storm to stream proccessing the Tweets that receives from an App that get tweets from the Stream Api from Twitter.

Both folders "masterStrom" and "twitterApp" are Eclipse projects, that use maven.

-twitterApp
	This project generate a class that connect to twitter streaming api, or read tweets from a file, where each line contains a tweet.
	To run this class, you need to give the parameters:
	-IP direcction where the server is going to run
	-Port of the server
	-Mode of processing (1=File, 2=Streaming API)
	-Name of the file (Ex. tweets.txt)
	-User Key (From the twitter App)
	-User secret (From the twitter App)
	-Token (From the twitter App)
	-Token secret (From the twitter App)

	First you need to create an App in twitter https://apps.twitter.com/
	For more documentain about the API https://dev.twitter.com/streaming/overview
	To change the query to receive the tweets, you have to modify the variable "String filter"

-masterStorm
	This project have the classes that proccess the tweets.
	It uses Apache storm https://storm.apache.org/
	To run this project, the main class is TopKTopology.java
	To execute, it needs the parameters:
	-IP direcction where the server is running
	-Port of the server
	-List with countries that you want to know the Top K (Ex. es,en,jp)

	The output of this aplication is a file with a line with the Top 3 words of each language, yocan modify this parameter in the source code, in "ReduceBolt.java"
	Example of the output
		[es,word1,word2,word3],[en,word1,word2,word3],[jp,word1,,]
	
	To change the length of the "window" for the top words, you have to change the parameter in the class "TopKTopology.java", line 27 
	"builder.setBolt("mapBolt", new MapBolt(600,200), 3)" being the 600, the seconds of the window, so you can change to your length.
	You can run this aplication in a server or in a local machine. 
	To run in the server, you have to run:
		StormSubmitter.submitTopology("TopKTopology", conf,builder.createTopology());
	To run in a local cluster, you have to run:
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("TopKTopology", conf,builder.createTopology());
		
	If you want, you should read the documentation of Storm to run it.

	
	
