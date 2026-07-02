package fr.jdiot.dev.flux.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.core.Acknowledgement.Status;

public class AcknowledgementTest {

  @Test
  public void testAcknowledgementBuilder() {
    final Acknowledgement ack = Acknowledgement.builder().fluxId("1234-abcd").status(Status.SUCCESS).receivedChunks(10)
        .totalBytes(1024L).timestamp("2026-06-27T10:30:00Z").reason("OK").build();

    Assertions.assertEquals("1234-abcd", ack.getFluxId());
    Assertions.assertEquals(Status.SUCCESS, ack.getStatus());
    Assertions.assertEquals(10, ack.getReceivedChunks());
    Assertions.assertEquals(1024L, ack.getTotalBytes());
    Assertions.assertEquals("2026-06-27T10:30:00Z", ack.getTimestamp());
    Assertions.assertEquals("OK", ack.getReason());
  }
}
