package danch.kafka.eve_static_load

import java.util

import collection.JavaConverters._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.yaml.snakeyaml.Yaml

class YamlToAvroEveType {
  private val schema = getSchema
  private val yamlParser = new Yaml
  private var recordCount = 0

  def yamlToAvro(record: String) : GenericRecord = {
    val deYaml = yamlParser.load[java.util.Map[Integer, util.HashMap[String, Any]]](record).asScala
    val avroRecord = new GenericData.Record(schema)
    for ((id, map) <- deYaml) {
      avroRecord.put("id", id)
      avroRecord.put("groupId", map.get("groupID"))
      avroRecord.put("published", map.get("published"))
      avroRecord.put("marketGroupId", map.get("marketGroupId"))
      val namesByLanguage = map.get("name").asInstanceOf[util.HashMap[String, String]].asScala
      var avroMap = new util.HashMap[String, String]()
      for ((key, value) <- namesByLanguage) {
        avroMap.put(key, value)
      }
      avroRecord.put("names", avroMap)
      avroRecord.put("marketGroupId", map.get("marketGroupId"))
    }
    recordCount += 1
    if ((recordCount % 100)==0) {
      println(s"YamlToAvroEveType mapped $recordCount records")
    }
    avroRecord
  }

  def getSchema : Schema = {
    val schemaStream = getClass.getResourceAsStream("/EveType.avsc")
    val schemaParser = new Schema.Parser
    schemaParser.parse(schemaStream)
  }
}
