package fr.jdiot.dev.flux.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class AcknowledgementTest {

    @Test
    public void testAcknowledgementBuilder() {
        Acknowledgement ack = Acknowledgement.builder()
                .fluxId("1234-abcd")
                .status("SUCCESS")
                .receivedChunks(10)
                .totalBytes(1024L)
                .timestamp("2026-06-27T10:30:00Z")
                .message("OK")
                .build();
                
        assertEquals("1234-abcd", ack.getFluxId());
        assertEquals("SUCCESS", ack.getStatus());
        assertEquals(10, ack.getReceivedChunks());
        assertEquals(1024L, ack.getTotalBytes());
        assertEquals("2026-06-27T10:30:00Z", ack.getTimestamp());
        assertEquals("OK", ack.getMessage());
    }
}
