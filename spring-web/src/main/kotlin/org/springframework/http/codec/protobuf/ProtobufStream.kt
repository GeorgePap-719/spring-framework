package org.springframework.http.codec.protobuf

import java.io.BufferedOutputStream
import java.io.OutputStream

/*
 * These can also be accessed through `kotlin.reflect.jvm.internal.impl.protobuf`, not sure
 * if it's ideal or not.
 */

internal fun ByteArray.writeDelimitedTo(outputStream: OutputStream) {
    val serializedSize = this.size
    val bufferSize = computePreferredBufferSize(computeUInt32SizeNoTag(serializedSize) + serializedSize)
    // not 100% sure it's needed, but at least can enforce the desired buffer-size
    val stream = outputStream.buffered(
        maxOf(
            bufferSize,
            MAX_VARINT_SIZE * 2
        )
    ) // simulate  CodedOutputStream.newInstance(output, bufferSize)
    stream.writeUInt32NoTag(serializedSize)
    writeTo(stream)
    stream.flush()
}

internal fun ByteArray.writeTo(outputStream: OutputStream) {
    val bufferSize = computePreferredBufferSize(this.size)
    val stream = outputStream.buffered(
        maxOf(
            bufferSize,
            MAX_VARINT_SIZE * 2
        )
    ) // simulate  CodedOutputStream.newInstance(output, bufferSize)
    stream.write(this)
    stream.flush()
}

private const val DEFAULT_BUFFER_SIZE = 4096
private const val MAX_VARINT_SIZE = 10

/** Returns the buffer size to efficiently write dataLength bytes to this CodedOutputStream. */
private fun computePreferredBufferSize(dataLength: Int): Int =
    if (dataLength > DEFAULT_BUFFER_SIZE) DEFAULT_BUFFER_SIZE else dataLength

/** Compute the number of bytes that would be needed to encode an uint32 field. */
private fun computeUInt32SizeNoTag(value: Int): Int = when {
    value and (0.inv() shl 7) == 0 -> 1
    value and (0.inv() shl 14) == 0 -> 2
    value and (0.inv() shl 21) == 0 -> 3
    value and (0.inv() shl 28) == 0 -> 4
    else -> 5
}

internal fun BufferedOutputStream.writeUInt32NoTag(size: Int) {
    var value = size
    try {
        while (true) {
            if ((value and 0x7F.inv() == 0)) {
                write(value)
                return
            } else {
                write((value and 0x7F) or 0x80)
                value ushr 7
                value = size ushr 7
            }
        }
    } catch (outOfBounds: IndexOutOfBoundsException) {
        throw IllegalArgumentException(outOfBounds)
    }
}