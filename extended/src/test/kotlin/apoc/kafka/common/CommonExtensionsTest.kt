package apoc.kafka.common

import apoc.kafka.extensions.toMap
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.Test
import kotlin.test.assertEquals

class CommonExtensionsTest {

    @Test
    fun `should convert AVRO record to Map`() {
        // given
        // this test generates a simple tree structure like this
        //           body
        //          /    \
        //         p     ul
        //               |
        //               li
        val BODY_SCHEMA = SchemaBuilder.builder("org.neo4j.example.html")
                .record("BODY").fields()
                .name("ul").type().array().items()
                .record("UL").namespace("org.neo4j.example.html").fields()
                .name("value").type().array().items()
                .record("LI").namespace("org.neo4j.example.html").fields()
                .optionalString("value")
                .name("class").type().nullable().array().items().stringType().noDefault()
                .endRecord().noDefault()
                .endRecord().noDefault()
                .name("p").type().array().items()
                .record("P").namespace("org.neo4j.example.html").fields()
                .optionalString("value")
                .endRecord().noDefault()
                .endRecord()
        val UL_SCHEMA = BODY_SCHEMA.getField("ul").schema().elementType
        val LI_SCHEMA = UL_SCHEMA.getField("value").schema().elementType
        val firstLi = listOf(
                GenericRecordBuilder(LI_SCHEMA).set("value", "First UL - First Element").set("class", null).build(),
                GenericRecordBuilder(LI_SCHEMA).set("value", "First UL - Second Element").set("class", listOf("ClassA", "ClassB")).build()
        )
        val secondLi = listOf(
                GenericRecordBuilder(LI_SCHEMA).set("value", "Second UL - First Element").set("class", null).build(),
                GenericRecordBuilder(LI_SCHEMA).set("value", "Second UL - Second Element").set("class", null).build()
        )
        val structUL = listOf(
                GenericRecordBuilder(UL_SCHEMA).set("value", firstLi).build(),
                GenericRecordBuilder(UL_SCHEMA).set("value", secondLi).build()
        )
        val structP = listOf(
                GenericRecordBuilder(BODY_SCHEMA.getField("p").schema().elementType).set("value", "First Paragraph").build(),
                GenericRecordBuilder(BODY_SCHEMA.getField("p").schema().elementType).set("value", "Second Paragraph").build()
        )
        val struct = GenericRecordBuilder(BODY_SCHEMA)
                .set("ul", structUL)
                .set("p", structP)
                .build()

        // when
        val actual = struct.toMap()

        // then
        val firstULMap = mapOf("value" to listOf(
                mapOf("value" to "First UL - First Element", "class" to null),
                mapOf("value" to "First UL - Second Element", "class" to listOf("ClassA", "ClassB"))))
        val secondULMap = mapOf("value" to listOf(
                mapOf("value" to "Second UL - First Element", "class" to null),
                mapOf("value" to "Second UL - Second Element", "class" to null)))
        val ulListMap = listOf(firstULMap, secondULMap)
        val pListMap = listOf(mapOf("value" to "First Paragraph"),
                mapOf("value" to "Second Paragraph"))
        val bodyMap = mapOf("ul" to ulListMap, "p" to pListMap)
        assertEquals(bodyMap, actual)
    }
}