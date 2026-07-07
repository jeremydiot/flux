package fr.jdiot.dev.flux.core;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;
import reactor.core.publisher.Flux;

@Getter
@Builder
public class FluxFile<M> {
  private final M metadata;
  private final long dataLength;
  private final Flux<ByteBuf> dataStream;
}
