package danch.kafka.eve_static_load

import java.util.Properties

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.{BufferedSource, Source}
import akka.stream.scaladsl.{Sink, Source => StreamSource}
import org.apache.avro.generic.GenericRecord

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class LoadTypes(source: BufferedSource, queue: SourceQueueWithComplete[String]) extends Runnable {

  def run : Unit = {
    var recordCount = 0;
    var documentBuilder = StringBuilder.newBuilder
    for (line <- source.getLines) {
      if (startOfRecord(line)) {
        if (documentBuilder.nonEmpty) {
          val document = documentBuilder.result()
          val success = queue.offer(document)
          Await.ready(success, 5.seconds) //better be overkill
          val actualCode = success.value.get.getOrElse(QueueOfferResult.Failure)
          if (actualCode != QueueOfferResult.enqueued) {
            println(s"File read Enqueue failed, ending early after $recordCount rows")
            return
          }
          recordCount += 1
          documentBuilder = StringBuilder.newBuilder
        }
        documentBuilder.append(line)
        documentBuilder.append("\n")
      } else {
        documentBuilder.append(line)
        documentBuilder.append("\n")
      }
    }
    println(s"File read ending normally after $recordCount rows")
  }

  private[this] def startOfRecord(line: String) : Boolean = {
    !line.matches("^[\\s\"\'].*") && line.length > 0
    //    if (isStart) {
    //      println(line)
    //    }
    //    isStart
  }
}

case class RecordMessage(record: GenericRecord)
object CompletionMessage
object LoadTypes extends App {


  val src = Source.fromFile("static_data/typeIDs.yaml")
  var counter = 0

  var yamlToAvro = new YamlToAvroEveType

  implicit val system: ActorSystem = ActorSystem("QuickStart")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val senderRef = system.actorOf(Props[KafkaSink])

  var flowDescriptor = StreamSource.queue[String](100, OverflowStrategy.backpressure)
    .map[GenericRecord](yamlToAvro.yamlToAvro(_))
    .map(new RecordMessage(_))
    .async
    .to(Sink.actorRef(senderRef, CompletionMessage ))


  var queue = flowDescriptor.run()

  val startTime = System.currentTimeMillis()
  Await.ready(Future {
      new LoadTypes(src, queue).run
  }, 1.hour)

  queue.complete()
  val endTime = System.currentTimeMillis()

  val terminationResult = system.terminate()
  Await.ready(terminationResult, 20.seconds)
  println(s"Processed all records in ${endTime - startTime} millis")
}

class KafkaSink extends Actor {

  val topic = "eve_types_avro"
  val brokers = "localhost:9092"
  val props = new Properties()
  var recordCount = 0
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "eve_static_load")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
  props.put("schema.registry.url", "http://localhost:8081")

  val producer = new KafkaProducer[String, GenericRecord](props)

  def receive = {
    case record: RecordMessage => {
      producer.send(new ProducerRecord[String, GenericRecord](topic, record.record.get("id").toString, record.record))
      recordCount += 1
    }
    case CompletionMessage => {
      println(s"KafkaProducer closing after $recordCount messages")
      producer.flush()
      producer.close()
    }
  }
}