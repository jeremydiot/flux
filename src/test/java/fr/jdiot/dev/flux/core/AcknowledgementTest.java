package fr.jdiot.dev.flux.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.core.ack.Acknowledgement;
import fr.jdiot.dev.flux.core.ack.Status;

public class AcknowledgementTest {

  @Test
  public void testAcknowledgementBuilder() {
    final Acknowledgement ack = new Acknowledgement("1234-abcd", Status.SUCCESS, 10, 1024L, 50L, 150L, 10L, 20L, 100L,
        5L, 10L, 300L, 15L, "OK");

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
