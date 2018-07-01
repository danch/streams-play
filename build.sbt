name := "streams-play"

version := "0.1"

scalaVersion := "2.12.6"

resolvers += "confluent" at "http://packages.confluent.io/maven/"

val kafka_streams_scala_version = "0.2.1"
val snakeyaml_version = "1.21"
val avro_serializer_version = "3.2.1"

libraryDependencies ++= Seq("com.lightbend" %%
  "kafka-streams-scala" % kafka_streams_scala_version)

libraryDependencies += "org.yaml" % "snakeyaml" % snakeyaml_version
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % avro_serializer_version
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.13"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.13"