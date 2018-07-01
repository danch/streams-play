package danch.kafka.eve_static_load

import java.util

import org.scalatest._

class YamlToAvroEveTypeSpec extends FlatSpec with Matchers {
  "YamlToAvroEveType" should "convert a simple Eve Type Yaml into a generic record" in {
    val converter = new YamlToAvroEveType
    val simpleYaml = """0:
                       |    groupID: 0
                       |    mass: 1.0
                       |    name:
                       |        de: '#System'
                       |        en: '#System'
                       |        fr: '#Système'
                       |        ja: '#システム'
                       |        ru: '#Система'
                       |        zh: '#星系'
                       |    portionSize: 1
                       |    published: false""".stripMargin
    val genericRecord = converter.yamlToAvro(simpleYaml)
    genericRecord should not be null
    genericRecord.get("groupId").asInstanceOf[Int] shouldEqual (0)
    genericRecord.get("names").asInstanceOf[util.HashMap[String, String]].get("en") shouldEqual "#System"
  }

  "YamlToAvroEveType" should "convert a Eve Type Yaml with blank lines into a generic record" in {
    val converter = new YamlToAvroEveType
    val simpleYaml = """0:
                       |    groupID: 0
                       |    mass: 1.0
                       |    name:
                       |        de: 'German text that inexplicably has
                       |
                       |
                       |        several blank lines in the middle'
                       |        en: 'English text with
                       |        linebreaks, but no blanks'
                       |    portionSize: 1
                       |    published: false""".stripMargin
    val genericRecord = converter.yamlToAvro(simpleYaml)
    genericRecord should not be null
    genericRecord.get("groupId").asInstanceOf[Int] shouldEqual (0)
    genericRecord.get("names").asInstanceOf[util.HashMap[String, String]].get("en") should not be null
  }

  "YamlToAvroEveType" should "convert a Eve Type Yaml with a line beginning with a single quote into a generic record" in {
    val converter = new YamlToAvroEveType
    val simpleYaml = """0:
                       |    groupID: 0
                       |    mass: 1.0
                       |    name:
                       |        de: 'German text that inexplicably has
                       |
                       |
                       |        several blank lines in the middle'
                       |        en: 'English text with
                       |        linebreaks, but no blanks and a bizzare orphanned quote
                       |'
                       |    portionSize: 1
                       |    published: false""".stripMargin
    val genericRecord = converter.yamlToAvro(simpleYaml)
    genericRecord should not be null
    genericRecord.get("groupId").asInstanceOf[Int] shouldEqual (0)
    genericRecord.get("names").asInstanceOf[util.HashMap[String, String]].get("en") should not be null
  }
}
