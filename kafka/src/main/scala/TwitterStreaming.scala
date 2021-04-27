import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.collection.JavaConverters._


object TwitterStreaming {
  def main(args: Array[String]): Unit = {

    if (args.length < 6) {
      //      println("Correct usage: Program_Name inputTopic outputTopic")
      println("Correct usage: Program_Name twitterConsumerKey twitterConsumerSecretKey twitterAccessToken twitterAccessTokenSecret KafkaTopic keyword")
      System.exit(1)
    }

    val twitterConsumerKey = args(0).toString
    val twitterConsumerSecretKey = args(1).toString
    val twitterAccessToken = args(2).toString
    val twitterAccessTokenSecret = args(3).toString
    val topic = args(4).toString
    val filters = args.slice(5, args.length)

    val kafkaBrokers = "localhost:9092,localhost:9093"
    val windowDuration = Seconds(10)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Creating a configuration builder using the provided Twitter Credentials
    val builder = new ConfigurationBuilder()
    builder.setOAuthConsumerKey(twitterConsumerKey)
    builder.setOAuthConsumerSecret(twitterConsumerSecretKey)
    builder.setOAuthAccessToken(twitterAccessToken)
    builder.setOAuthAccessTokenSecret(twitterAccessTokenSecret)
    val configuration = builder.build()

    val sparkConfiguration = new SparkConf().
      setAppName("TwitterAnalysis").
      setMaster(sys.env.getOrElse("spark.master", "local[*]"))

    val sparkContext = new SparkContext(sparkConfiguration)

    // Creating a streaming context with the specifies Window Size
    val streamingContext = new StreamingContext(sparkContext, windowDuration)

    // Creating an authorization variable using the configuration builder variable.
    val auth = Some(new OAuthAuthorization(configuration))

    /**
     * Creating a DStream to fetch the Tweets from Twitter.
     */
    val tweets: DStream[Status] =
      TwitterUtils.createStream(streamingContext, auth, filters)

    /**
     * Here we extract the text from the tweets and find the sentiment of each of them using the getSentiment method.
     * We then store the tweet text and the sentiment in the rdd.
     */
    val textAndSentiments: DStream[(String, String)] =
      tweets.filter(status => status.getLang == "en")
        .map(_.getText)
        .map(tweetText => (tweetText, getSentiment(tweetText)))

    /**
     * Printing the value of sentiment with message on the console.
     */
    textAndSentiments.foreachRDD(rdd => rdd.collect().foreach(row => println(" Sentiment -> " + row._2 + " TweetMessage -> " + row._1)))

    /**
     * Send Data to Kafka Broker
     */
    textAndSentiments.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {
        /**
         * Print statements in this section are shown in the executor's stdout logs.
         */
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)

        partition.foreach(record => {
          val message = record._1.toString
          val sentiment = record._2.toString
          /**
           * Sending the sentiment of the tweet to the consumer console.
           */
          val tweetMessage = new ProducerRecord[String, String](topic, message, sentiment)
          producer.send(tweetMessage)
        })
        producer.close()
      })

    })

    /**
     * Start the streaming using streaming context.
     */
    streamingContext.start()

    /**
     * Make the Stream run forever until it is made to stop.
     */
    streamingContext.awaitTermination()
  }

  def getSentiment(tweets: String): String = {
    var mainSentiment = 0
    var longest = 0
    val sentimentText = Array("Very Negative", "Negative", "Neutral", "Positive", "Very Positive")
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    new StanfordCoreNLP(props).process(tweets).get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.foreach((sentence: CoreMap) => {
      val sentiment = RNNCoreAnnotations.getPredictedClass(sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree]))
      val partText = sentence.toString
      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }
    })
    sentimentText(mainSentiment)
  }
}


