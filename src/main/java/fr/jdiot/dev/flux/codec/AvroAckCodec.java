package fr.jdiot.dev.flux.codec;

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

import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Avro-based implementation of FluxCodec. Optimized for maximum speed and
 * minimal memory footprint using direct binary encoders/decoders.
 */
public class AvroAckCodec implements AckCodec {

  private final Class<Acknowledgement> type = Acknowledgement.class;
  private final Schema schema;
  private final DatumWriter<Acknowledgement> writer;
  private final DatumReader<Acknowledgement> reader;

  public AvroAckCodec() {
    if (SpecificRecordBase.class.isAssignableFrom(this.type)) {
      // from .avsc file
      this.schema = SpecificData.get().getSchema(this.type);
      this.writer = new SpecificDatumWriter<>(this.schema);
      this.reader = new SpecificDatumReader<>(this.schema);
    } else {
      // from java pojo
      this.schema = ReflectData.AllowNull.get().getSchema(this.type);
      this.writer = new ReflectDatumWriter<>(this.schema);
      this.reader = new ReflectDatumReader<>(this.schema);
    }
  }

  @Override
  public ByteBuf encode(final Acknowledgement object) {
    try {
      final ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
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
  public Acknowledgement decode(final byte[] bytes) {
    try {
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
      return this.reader.read(null, decoder);
    } catch (final Exception e) {
      throw new FluxException("Failed to decode byte array to " + this.type.getName(), e);
    }
  }
}
