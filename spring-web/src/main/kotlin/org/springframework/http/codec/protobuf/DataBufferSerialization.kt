package org.springframework.http.codec.protobuf

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.protobuf.ProtoBuf
import org.reactivestreams.Publisher
import org.springframework.core.codec.DecodingException
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.core.io.buffer.DataBufferFactory
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.core.io.buffer.DataBufferUtils
import org.springframework.util.FastByteArrayOutputStream
import reactor.core.publisher.Mono
import java.io.IOException

/*
 * kotlinx.serialization throws mostly IllegalArgumentException, maybe catch
 * this instead of IOException and then throw IllegalStateException?
 */

@ExperimentalSerializationApi
internal fun <T> ProtoBuf.encodeToDataBuffer(
    serializer: SerializationStrategy<T>,
    value: T,
    dataBufferFactory: DataBufferFactory
): DataBuffer = try {
    val outputStream = FastByteArrayOutputStream()
    val protoBytes = encodeToByteArray(serializer, value)
    protoBytes.writeTo(outputStream)
    // use wrap() here which does now allocate new memory, that's
    // why we do not need DataBufferUtils.release(buffer).
    dataBufferFactory.wrap(outputStream.toByteArrayUnsafe())
} catch (ioException: IOException) {
    throw IllegalStateException("Unexpected I/O error while writing to data buffer", ioException)
}

@ExperimentalSerializationApi
internal fun <T> ProtoBuf.encodeToDataBufferDelimited(
    serializer: SerializationStrategy<T>,
    value: T,
    dataBufferFactory: DataBufferFactory
): DataBuffer = try {
    val outputStream = FastByteArrayOutputStream()
    val protoBytes = encodeToByteArray(serializer, value)
    protoBytes.writeDelimitedTo(outputStream)
    // use wrap() here which does now allocate new memory, that's
    // why we do not need DataBufferUtils.release(buffer).
    dataBufferFactory.wrap(outputStream.toByteArrayUnsafe())
} catch (ioException: IOException) {
    throw IllegalStateException("Unexpected I/O error while writing to data buffer", ioException)
}

@ExperimentalSerializationApi
internal fun <T> ProtoBuf.decodeFromDataBuffer(
    deserializer: DeserializationStrategy<T>,
    dataBuffer: DataBuffer
): T {
    try {
        return decodeFromByteArray(deserializer, dataBuffer.asInputStream().readAllBytes())
    } finally {
        DataBufferUtils.release(dataBuffer)
    }
}

@ExperimentalSerializationApi
internal fun <T> ProtoBuf.decodeFromDataBufferToMono(
    deserializer: DeserializationStrategy<T>,
    inputStream: Publisher<DataBuffer>
): Mono<T> = Mono.from(inputStream).map {
    decodeFromDataBuffer(deserializer, it)
}

@ExperimentalSerializationApi
internal class ProtobufDataBufferDecoder(
    private val protobuf: ProtoBuf,
    private val messageSize: Int = DEFAULT_MESSAGE_MAX_SIZE
) {
    private var offset: Int = 0
    private var messageBytesToRead: Int = 0

    fun <T> decodeDelimitedMessages(
        deserializer: DeserializationStrategy<List<T>>,
        input: DataBuffer
    ): List<T> = buildList {
        var remainingBytesToRead: Int
        var chunkBytesToRead: Int
        try {
            do {
                if (!readMessageSize(input)) return@buildList
                if (messageSize in 1 until messageBytesToRead) {
                    throw DataBufferLimitException(
                        "The number of bytes to read for message ($messageBytesToRead) exceeds the configured limit ($messageSize)"
                    )
                }
                val buffer = input.factory().allocateBuffer(messageBytesToRead)

                chunkBytesToRead = minOf(messageBytesToRead, input.readableByteCount())
                remainingBytesToRead = input.readableByteCount() - chunkBytesToRead

                val bytesToWrite = ByteArray(chunkBytesToRead)
                input.read(bytesToWrite, offset, chunkBytesToRead)
                buffer.write(bytesToWrite)
                messageBytesToRead -= chunkBytesToRead
                if (messageBytesToRead == 0) { // do not deserialize in case readableByteCount was smaller than messageBytesToRead
                    DataBufferUtils.release(buffer)
                    val messages = protobuf.decodeFromByteArray(deserializer, buffer.asInputStream().readAllBytes())
                    addAll(messages)
                }
            } while (remainingBytesToRead > 0)
        } finally {
            DataBufferUtils.release(input)
        }
    }

    // similar impl to readMessageSize from ProtobufDecoder.java
    private fun readMessageSize(input: DataBuffer): Boolean {
        if (offset == 0) {
            if (input.readableByteCount() == 0) return false
            val messageSize = input.read().toInt() // should be the message's size
            if (messageSize and 0x80 == 0) { // check if it's positive? Why tho? in case the first byte was not the message size?
                messageBytesToRead = messageSize
                return true
            }
            messageBytesToRead = messageSize and 0x7f // does this drops the msb?
            offset = 7
        }
        if (offset < 32) {
            while (offset < 32) {
                if (input.readableByteCount() == 0) return false
                val messageSize = input.read().toInt()
                // shift the groups of 7 bits  because varints store numbers with the least significant group first
                messageBytesToRead =
                    (messageBytesToRead or messageSize and 0x7f) shl offset // and concatenate them together
                if (messageSize and 0x80 == 0) {
                    offset = 0
                    return true
                }
                offset += 7
            }
        }
        // Keep reading up to 64 bits
        //Note: even though we read more bytes why we do not store their message size?
        while (offset < 64) {
            if (input.readableByteCount() == 0) return false
            val messageSize = input.read().toInt()
            if (messageSize and 0x80 == 0) {
                offset = 0
                return true
            }
            offset += 7
        }
        offset = 0 // is this needed?
        throw DecodingException("Cannot parse message size: malformed varint")
    }

    companion object {
        private const val DEFAULT_MESSAGE_MAX_SIZE = 256 * 1024
    }
}