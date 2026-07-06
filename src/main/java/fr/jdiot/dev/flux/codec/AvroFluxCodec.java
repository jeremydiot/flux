package fr.jdiot.dev.flux.codec;

import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

/**
 * Avro-based implementation of FluxCodec. Optimized for maximum speed and
 * minimal memory footprint using direct binary encoders/decoders.
 */
public class AvroFluxCodec<T> implements FluxCodec<T> {

  private final Class<T> type;
  private final Schema schema;
  private final DatumWriter<T> writer;
  private final DatumReader<T> reader;

  public AvroFluxCodec(final Class<T> type) {
    this.type = type;
    if (SpecificRecordBase.class.isAssignableFrom(type)) {
      this.schema = SpecificData.get().getSchema(type);
      this.writer = new SpecificDatumWriter<>(this.schema);
      this.reader = new SpecificDatumReader<>(this.schema);
    } else {
      this.schema = ReflectData.AllowNull.get().getSchema(type);
      this.writer = new ReflectDatumWriter<>(this.schema);
      this.reader = new ReflectDatumReader<>(this.schema);
    }
  }

  @Override
  public ByteBuf encode(final T object) {
    try {
      final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
      try (ByteBufOutputStream out = new ByteBufOutputStream(buf)) {
        final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        this.writer.write(object, encoder);
        encoder.flush();
      } catch (final Exception inner) {
        buf.release();
        throw inner;
      }
      return buf;
    } catch (final Exception e) {
      throw new FluxException("Failed to encode object of type " + this.type.getName(), e);
    }
  }

  @Override
  public T decode(final ByteBuf buffer) {
    try {
      try (InputStream is = new ByteBufInputStream(buffer)) {
        final BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(is, null);
        return this.reader.read(null, decoder);
      }
    } catch (final Exception e) {
      throw new FluxException("Failed to decode ByteBuf to " + this.type.getName(), e);
    }
  }
}
