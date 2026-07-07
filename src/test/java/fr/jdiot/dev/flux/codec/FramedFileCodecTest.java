package fr.jdiot.dev.flux.codec;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.core.FluxFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class FramedFileCodecTest {

  @Test
  void testEncodeAndDecodeFlux() {
    final PojoCodec<String> stringCodec = new AvroPojoCodec<>(String.class);
    final FramedFileCodec<String> framedCodec = new FramedFileCodec<>(stringCodec);

    // Create a FluxFile
    final String metadata = "image_metadata";
    final byte[] fileData = new byte[] { 1, 2, 3, 4, 5 };

    final FluxFile<String> file1 = FluxFile.<String>builder().metadata(metadata).dataLength(fileData.length)
        .dataStream(Flux.just(Unpooled.wrappedBuffer(fileData))).build();

    final String metadata2 = "video_metadata";
    final byte[] fileData2 = new byte[] { 9, 8, 7 };

    final FluxFile<String> file2 = FluxFile.<String>builder().metadata(metadata2).dataLength(fileData2.length)
        .dataStream(Flux.just(Unpooled.wrappedBuffer(fileData2))).build();

    // Encode
    final Flux<ByteBuf> encodedStream = framedCodec.encode(Flux.just(file1, file2));

    // Decode
    final Flux<FluxFile<String>> decodedStream = framedCodec.decode(encodedStream);

    StepVerifier.create(decodedStream
        .concatMap(decodedFile -> decodedFile.getDataStream().reduce(new ArrayList<Byte>(), (list, buf) -> {
          while (buf.readableBytes() > 0)
            list.add(buf.readByte());
          buf.release();
          return list;
        }).map(bytes -> new AbstractMap.SimpleEntry<>(decodedFile, bytes)))).assertNext(entry -> {
          final FluxFile<String> decodedFile = entry.getKey();
          final List<Byte> bytes = entry.getValue();

          Assertions.assertEquals("image_metadata", decodedFile.getMetadata());
          Assertions.assertEquals(5, decodedFile.getDataLength());

          Assertions.assertEquals(5, bytes.size());
          Assertions.assertEquals(1, bytes.get(0).byteValue());
          Assertions.assertEquals(2, bytes.get(1).byteValue());
          Assertions.assertEquals(3, bytes.get(2).byteValue());
          Assertions.assertEquals(4, bytes.get(3).byteValue());
          Assertions.assertEquals(5, bytes.get(4).byteValue());
        }).assertNext(entry -> {
          final FluxFile<String> decodedFile = entry.getKey();
          final List<Byte> bytes = entry.getValue();

          Assertions.assertEquals("video_metadata", decodedFile.getMetadata());
          Assertions.assertEquals(3, decodedFile.getDataLength());

          Assertions.assertEquals(3, bytes.size());
          Assertions.assertEquals(9, bytes.get(0).byteValue());
          Assertions.assertEquals(8, bytes.get(1).byteValue());
          Assertions.assertEquals(7, bytes.get(2).byteValue());
        }).verifyComplete();
  }

  @Test
  void testNetworkFragmentation() {
    final PojoCodec<String> stringCodec = new AvroPojoCodec<>(String.class);
    final FramedFileCodec<String> framedCodec = new FramedFileCodec<>(stringCodec);

    final String metadata = "fragmented_file";
    final byte[] fileData = new byte[100];
    for (int i = 0; i < 100; i++)
      fileData[i] = (byte) i;

    final FluxFile<String> file1 = FluxFile.<String>builder().metadata(metadata).dataLength(fileData.length)
        .dataStream(Flux.range(0, (fileData.length + 4) / 5)
            .map(i -> Unpooled.wrappedBuffer(fileData, i * 5, Math.min(5, fileData.length - i * 5)))).build();

    final Flux<ByteBuf> encodedStream = framedCodec.encode(Flux.just(file1));

    // Simulate network fragmentation by splitting into 1-byte chunks
    final Flux<ByteBuf> fragmentedStream = encodedStream.concatMapIterable(buf -> {
      final List<ByteBuf> chunks = new ArrayList<>();
      while (buf.readableBytes() > 0) {
        chunks.add(buf.readRetainedSlice(1));
      }
      buf.release();
      return chunks;
    });

    final Flux<FluxFile<String>> decodedStream = framedCodec.decode(fragmentedStream);

    StepVerifier.create(decodedStream.concatMap(decodedFile -> decodedFile.getDataStream().reduce(0, (count, buf) -> {
      count += buf.readableBytes();
      buf.release();
      return count;
    }).map(count -> new AbstractMap.SimpleEntry<>(decodedFile, count)))).assertNext(entry -> {
      final FluxFile<String> decodedFile = entry.getKey();
      final Integer count = entry.getValue();

      Assertions.assertEquals("fragmented_file", decodedFile.getMetadata());
      Assertions.assertEquals(100, decodedFile.getDataLength());
      Assertions.assertEquals(100, count);
    }).verifyComplete();
  }
}
