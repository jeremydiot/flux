package fr.jdiot.dev.flux.core;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * Represents the acknowledgement structure defined in the Technical Specification.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Acknowledgement {
    private String fluxId;
    private String status;
    private int receivedChunks;
    private long totalBytes;
    private String timestamp;
    private String message;
}
