package fr.jdiot.dev.flux.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.core.Acknowledgement.Status;

public class AcknowledgementTest {

  @Test
  public void testAcknowledgementBuilder() {
    final Acknowledgement ack = Acknowledgement.builder().fluxId("1234-abcd").status(Status.SUCCESS).nbElement(10)
        .totalBytes(1024L).serverPreProcessingTimeMs(50L).serverProcessingTimeMs(150L).serverPostProcessingTimeMs(10L)
        .pullClientPreProcessingTimeMs(20L).pullClientProcessingTimeMs(100L).pullClientPostProcessingTimeMs(5L)
        .pushClientPreProcessingTimeMs(10L).pushClientProcessingTimeMs(300L).pushClientPostProcessingTimeMs(15L)
        .reason("OK").build();

    Assertions.assertEquals("1234-abcd", ack.getFluxId());
    Assertions.assertEquals(Status.SUCCESS, ack.getStatus());
    Assertions.assertEquals(10, ack.getNbElement());
    Assertions.assertEquals(1024L, ack.getTotalBytes());
    Assertions.assertEquals(50L, ack.getServerPreProcessingTimeMs());
    Assertions.assertEquals(150L, ack.getServerProcessingTimeMs());
    Assertions.assertEquals(10L, ack.getServerPostProcessingTimeMs());
    Assertions.assertEquals(20L, ack.getPullClientPreProcessingTimeMs());
    Assertions.assertEquals(100L, ack.getPullClientProcessingTimeMs());
    Assertions.assertEquals(5L, ack.getPullClientPostProcessingTimeMs());
    Assertions.assertEquals(10L, ack.getPushClientPreProcessingTimeMs());
    Assertions.assertEquals(300L, ack.getPushClientProcessingTimeMs());
    Assertions.assertEquals(15L, ack.getPushClientPostProcessingTimeMs());
    Assertions.assertEquals("OK", ack.getReason());
  }
}
