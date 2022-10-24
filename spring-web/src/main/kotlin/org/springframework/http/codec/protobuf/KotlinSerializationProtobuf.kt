package org.springframework.http.codec.protobuf

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.protobuf.ProtoBuf
import kotlinx.serialization.serializer
import kotlinx.serialization.serializerOrNull
import org.reactivestreams.Publisher
import org.springframework.core.ResolvableType
import org.springframework.core.codec.Decoder
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.core.io.buffer.DataBufferFactory
import org.springframework.http.MediaType
import org.springframework.http.codec.HttpMessageEncoder
import org.springframework.util.ConcurrentReferenceHashMap
import org.springframework.util.MimeType
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.lang.reflect.Type

/**
 * A serializer that encodes Messages using [Google Protocol Buffers](https://developers.google.com/protocol-buffers/])
 * with kotlin's [ProtoBuf] implementation.
 *
 * Flux are serialized using [delimited Protobuf messages](https://developers.google.com/protocol-buffers/docs/techniques?hl=en#streaming)
 * with the size of each message specified before the message itself. Single values are
 * serialized using regular Protobuf message format(without the size prepended before the
 * message).
 *
 * To generate a serializer for kotlin classes you need to install kotlin's serialization
 * plugin and to annotate your class with `@Serializable` annotation (for more see [ProtoBuf]).
 *
 * This encoder uses Proto2 specification and supports "application/x-protobuf" and
 * "application/octet-stream".
 */
@ExperimentalSerializationApi
class KotlinxSerializationProtobufEncoder(
    private val protobufSerializer: ProtoBuf = ProtoBuf
) : ProtobufCodecSupport(), HttpMessageEncoder<Any> {
    override fun canEncode(elementType: ResolvableType, mimeType: MimeType?): Boolean =
        serializerOrNull(elementType.type) != null && supportsMimeType(mimeType)

    override fun encode(
        inputStream: Publisher<out Any>,
        bufferFactory: DataBufferFactory,
        elementType: ResolvableType,
        mimeType: MimeType?,
        hints: MutableMap<String, Any>?
    ): Flux<DataBuffer> {
        val kSerializer = serializer(elementType.type)
        return Flux.from(inputStream)
            .map { protobufSerializer.encodeToDataBufferDelimited(kSerializer, it, bufferFactory) }
    }

    override fun encodeValue(
        value: Any,
        bufferFactory: DataBufferFactory,
        valueType: ResolvableType,
        mimeType: MimeType?,
        hints: MutableMap<String, Any>?
    ): DataBuffer {
        val kSerializer = getSerializer(protobufSerializer, valueType.type)
        return protobufSerializer.encodeToDataBuffer(kSerializer, value, bufferFactory)
    }

    override fun getEncodableMimeTypes(): MutableList<MimeType> = mimeTypes

    override fun getStreamingMediaTypes(): MutableList<MediaType> = MIME_TYPES.map {
        MediaType(it.type, it.subtype, mapOf(DELIMITED_KEY to DELIMITED_VALUE))
    }.toMutableList()
}

/**
 * A serializer that decodes Messages using [Google Protocol Buffers](https://developers.google.com/protocol-buffers/])
 * with kotlin's [ProtoBuf] implementation.
 *
 * Flux are deserialized via [decode] are expected to
 * use [delimited Protobuf messages](https://developers.google.com/protocol-buffers/docs/techniques?hl=en#streaming)
 * with the size of each message specified before the message itself. Single values
 * deserialized via [decodeToMono] are expected to use regular Protobuf message format
 * (without the size prepended before the message).
 *
 * To generate a deserializer for kotlin classes you need to install kotlin's serialization
 * plugin and to annotate your class with `@Serializable` annotation (for more see [ProtoBuf]).
 *
 * This decoder uses Proto2 specification and supports "application/x-protobuf" and
 * "application/octet-stream".
 */
@ExperimentalSerializationApi
class KotlinxSerializationProtobufDecoder(
    private val protobufSerializer: ProtoBuf = ProtoBuf,
) : ProtobufCodecSupport(), Decoder<Any> {
    var maxMessageSize: Int = 256 * 1024 // default max size for aggregating messages

    override fun canDecode(elementType: ResolvableType, mimeType: MimeType?): Boolean =
        serializerOrNull(elementType.type) != null && supportsMimeType(mimeType)

    override fun decode(
        inputStream: Publisher<DataBuffer>,
        elementType: ResolvableType,
        mimeType: MimeType?,
        hints: MutableMap<String, Any>?
    ): Flux<Any> {
        if (inputStream is Mono) {
            return Flux.from(decodeToMono(inputStream, elementType, mimeType, hints))
        }
        val decoder = ProtobufDataBufferDecoder(protobufSerializer, maxMessageSize)
        val kSerializer: KSerializer<Any> = getSerializer(protobufSerializer, elementType.type)
        val listSerializer = ListSerializer(kSerializer)
        //TODO: since this might return more than items, is `map` the right operator?
        // in coroutines we would use transform here instead of map
        return Flux.from(inputStream).map { decoder.decodeDelimitedMessages(listSerializer, it) }
    }

    override fun decodeToMono(
        inputStream: Publisher<DataBuffer>,
        elementType: ResolvableType,
        mimeType: MimeType?,
        hints: MutableMap<String, Any>?
    ): Mono<Any> {
        val kSerializer = getSerializer(protobufSerializer, elementType.type)
        return protobufSerializer.decodeFromDataBufferToMono(kSerializer, inputStream)
    }

    override fun decode(
        buffer: DataBuffer,
        targetType: ResolvableType,
        mimeType: MimeType?,
        hints: MutableMap<String, Any>?
    ): Any {
        val kSerializer = getSerializer(protobufSerializer, targetType.type)
        return protobufSerializer.decodeFromDataBuffer(kSerializer, buffer)
    }

    override fun getDecodableMimeTypes(): MutableList<MimeType> = mimeTypes
}

private val serializersCache = ConcurrentReferenceHashMap<Type, KSerializer<*>>()

@ExperimentalSerializationApi
private fun getSerializer(protobuf: ProtoBuf, type: Type): KSerializer<Any> =
    serializersCache.getOrPut(type) {
        protobuf.serializersModule.serializer(type)
    }.cast()

@Suppress("UNCHECKED_CAST", "NOTHING_TO_INLINE")
private inline fun KSerializer<*>.cast(): KSerializer<Any> {
    return this as KSerializer<Any>
}