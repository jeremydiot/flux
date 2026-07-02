package fr.jdiot.dev.flux.core;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
public class Acknowledgement {
  private String fluxId;
  private Status status;
  private int receivedChunks;
  private long totalBytes;
  private String timestamp;
  private String reason;

  private Acknowledgement(final String fluxId, final Status status, final int receivedChunks, final long totalBytes,
      final String timestamp, final String reason) {
    this.fluxId = fluxId;
    this.status = status;
    this.receivedChunks = receivedChunks;
    this.totalBytes = totalBytes;
    this.timestamp = timestamp;
    this.reason = reason;
  }

  public static Acknowledgement success(final String fluxId) {
    return new Acknowledgement(fluxId, Status.SUCCESS, 0, 0, null, null);
  }

  public static Acknowledgement failed(final String fluxId) {
    return new Acknowledgement(fluxId, Status.FAILED, 0, 0, null, null);
  }

  public static Acknowledgement partial(final String fluxId) {
    return new Acknowledgement(fluxId, Status.PARTIAL, 0, 0, null, null);
  }

  public enum Status {
    SUCCESS, FAILED, PARTIAL
  }
}
