package danch.kafka.eve_static_load

import java.util
import java.util.concurrent.Executors
import java.util.{Collections, Properties}

import collection.JavaConverters._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.yaml.snakeyaml.Yaml


class TypesToAvro {
  val inTopic = "eve_types_yaml"
  val outTopic = "eve_types_avro"
  val brokers = "localhost:9092"

  def run = {
    val producer = createProducer

    val consumer = createConsumer
    consumer.subscribe(Collections.singletonList(inTopic))
    val schema = getSchema

    Executors.newSingleThreadExecutor.execute(() => {
        val yamlParser = new Yaml
        var recordCount = 0;
        while (true) {
          //declarations here shouldn't be necessary, but idea kinda lost its mind
          val  records: ConsumerRecords[String, String] = consumer.poll(1000)

          records.forEach( (record) => {
//            println(record.value)
            try {
              val deYaml = yamlParser.load[java.util.Map[Integer, util.HashMap[String, Any]]](record.value).asScala
              val avroRecord = new GenericData.Record(schema)
              for ((id, map) <- deYaml) {
                avroRecord.put("id", id)
                avroRecord.put("groupId", map.get("groupID"))
                avroRecord.put("published", map.get("published"))
                avroRecord.put("marketGroupId", map.get("marketGroupId"))
                val namesByLanguage = (map.get("name").asInstanceOf[util.HashMap[String, String]]).asScala
                var avroMap = new util.HashMap[String, String]()
                for ((key, value) <- namesByLanguage) {
                  avroMap.put(key, value)
                }
                avroRecord.put("names", avroMap)
                avroRecord.put("marketGroupId", map.get("marketGroupId"))
              }

              producer.send(new ProducerRecord[String, GenericRecord](outTopic, avroRecord.get("id").toString, avroRecord))
              recordCount += 1
              if ((recordCount % 100) == 0) {
                println(s"processed $recordCount records")
              }
            } catch {
              case t: Throwable => println(s"Exception [${t.getMessage}] on  yaml \n${record.value}")
            }
          })
        }
    })
  }

  def createConsumer : KafkaConsumer[String, String] = {
    import org.apache.kafka.clients.consumer.KafkaConsumer
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "avroize-eve-types")
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("session.timeout.ms", "60000")
    new KafkaConsumer[String, String](props)
  }

  def createProducer : KafkaProducer[String, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    //props.put(ProducerConfig.CLIENT_ID_CONFIG, "eve_static_load")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", "http://localhost:8081")

    new KafkaProducer[String, GenericRecord](props)
  }

  def getSchema : Schema = {
    val schemaStream = getClass.getResourceAsStream("/EveType.avsc")
    val schemaParser = new Schema.Parser
    schemaParser.parse(schemaStream)
  }
}

object TypesToAvro extends App {
  val typesToAvro = new TypesToAvro
  typesToAvro.run
}