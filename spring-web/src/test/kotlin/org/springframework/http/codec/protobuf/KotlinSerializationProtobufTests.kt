package org.springframework.http.codec.protobuf

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoBuf
import kotlinx.serialization.serializer
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.core.ResolvableType.*
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.core.io.buffer.DataBufferUtils
import org.springframework.core.testfixture.codec.AbstractEncoderTests
import org.springframework.http.MediaType
import org.springframework.util.MimeType
import reactor.core.publisher.Mono
import java.io.IOException
import java.io.UncheckedIOException

/**
 * Tests for Protobuf using kotlinx.serialization.
 */
@OptIn(ExperimentalSerializationApi::class)
class KotlinxSerializationProtobufEncoderTests :
    AbstractEncoderTests<KotlinSerializationProtobufEncoder>(KotlinSerializationProtobufEncoder(protobuf)) {
    private val protobufMimeType = MimeType("application", "x-protobuf")

    @Test
    override fun canEncode() {
        val pojoType = forClass(Pojo::class.java)
        val nonSerializablePojo = forClass(NSPojo::class.java)
        assertThat(encoder.canEncode(pojoType, null)).isTrue
        assertThat(encoder.canEncode(pojoType, protobufMimeType)).isTrue
        assertThat(encoder.canEncode(pojoType, MediaType.APPLICATION_OCTET_STREAM)).isTrue
        assertThat(encoder.canEncode(pojoType, MediaType.APPLICATION_JSON)).isFalse
        assertThat(encoder.canEncode(nonSerializablePojo, protobufMimeType)).isFalse

        assertThat(encoder.canEncode(forClassWithGenerics(List::class.java, Int::class.java), protobufMimeType)).isTrue
        assertThat(encoder.canEncode(forClassWithGenerics(List::class.java, Pojo::class.java), protobufMimeType)).isTrue
        assertThat(
            encoder.canEncode(
                forClassWithGenerics(ArrayList::class.java, Int::class.java),
                protobufMimeType
            )
        ).isTrue
    }

    @Test
    override fun encode() {
        val msg1 = MsgK("Foo", MsgK2(123))
        val msg2 = MsgK("Bar", MsgK2(456))
        val elementType = forClass(MsgK::class.java)

        val input = Mono.just(msg1)
        testEncode(input, MsgK::class.java) { step ->
            step.consumeNextWith { dataBuffer ->
                try {
                    // Note: the encoding actually happens
//                    val decoder = ProtobufDataBufferDecoder(protobuf)
//                    val kSerializer: KSerializer<Any> = protobuf.serializersModule.serializer(elementType.type)
//                    val listSerializer = ListSerializer(kSerializer)
//                    val result = decoder.decodeDelimitedMessages(listSerializer, dataBuffer)
//                    println("Result: $result")
                    //assertThat()
                } catch (ioException: IOException) {
                    throw UncheckedIOException(ioException)
                }
                finally {
                    DataBufferUtils.release(dataBuffer)
                }
            }.verifyComplete()
        }
    }


    @Serializable
    data class Pojo(val foo: String? = null, val bar: String? = null, val pojo: Pojo? = null)

    data class NSPojo(val foo: String)

    @Serializable
    data class MsgK(val foo: String? = null, val msg2: MsgK2? = null)

    @Serializable
    data class MsgK2(val foo: Int? = null)
}

@OptIn(ExperimentalSerializationApi::class)
private val protobuf = ProtoBuf

@OptIn(ExperimentalSerializationApi::class) // needed to retrieve the serializer
private inline fun <reified T> ProtoBuf.decodeFromDataBuffer(
    dataBuffer: DataBuffer
): T = decodeFromDataBuffer(serializersModule.serializer(), dataBuffer)




