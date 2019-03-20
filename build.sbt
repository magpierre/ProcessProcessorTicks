name := "ProcessProcessorTicks"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "mapr" at "http://repository.mapr.com/maven"
resolvers += "xerces" at "https://repository.jboss.org/nexus/content/repositories/thirdparty-releases/"
retrieveManaged := true


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"   % "2.3.1-mapr-1808"
  , "org.apache.spark" %% "spark-sql"    % "2.3.1-mapr-1808"
  , "org.apache.spark" %% "spark-streaming" % "2.3.1-mapr-1808"
  , "org.apache.spark" %% "spark-streaming-kafka-0-9" % "2.3.1-mapr-1808"
  , "org.apache.kafka" % "kafka-clients" % "1.1.1-mapr-1808"
  , "com.mapr.streams" % "mapr-streams"  % "6.1.0-mapr"
  , "com.mapr.db" % "maprdb-cdc" % "6.1.0-mapr"
  , "com.mapr.db" %  "maprdb-spark" % "2.3.1-mapr-1808" //Remember single % signs to avoid having version of scala added s
)