package fr.jdiot.dev.flux.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class Acknowledgement {
  private String fluxId;
  private Status status;

  private int nbElement; // TODO
  private long totalBytes; // TODO

  private long serverPreProcessingTimeMs;
  private long serverProcessingTimeMs;
  private long serverPostProcessingTimeMs;

  private long pullClientPreProcessingTimeMs;
  private long pullClientProcessingTimeMs;
  private long pullClientPostProcessingTimeMs;

  private long pushClientPreProcessingTimeMs;
  private long pushClientProcessingTimeMs;
  private long pushClientPostProcessingTimeMs;

  private String reason;

  public static Acknowledgement success(final String fluxId) {
    return Acknowledgement.builder().fluxId(fluxId).status(Status.SUCCESS).build();
  }

  public static Acknowledgement failed(final String fluxId) {
    return Acknowledgement.builder().fluxId(fluxId).status(Status.FAILED).build();
  }

  public static Acknowledgement failed(final String fluxId, final String reason) {
    return Acknowledgement.builder().fluxId(fluxId).status(Status.FAILED).reason(reason).build();
  }

  public static Acknowledgement partial(final String fluxId) {
    return Acknowledgement.builder().fluxId(fluxId).status(Status.PARTIAL).build();
  }

  public enum Status {
    SUCCESS, FAILED, PARTIAL
  }

  public String printProcessingTimes() {
    final long serverTotal = this.serverPreProcessingTimeMs + this.serverProcessingTimeMs
        + this.serverPostProcessingTimeMs;
    final long pullClientTotal = this.pullClientPreProcessingTimeMs + this.pullClientProcessingTimeMs
        + this.pullClientPostProcessingTimeMs;
    final long pushClientTotal = this.pushClientPreProcessingTimeMs + this.pushClientProcessingTimeMs
        + this.pushClientPostProcessingTimeMs;

    final StringBuilder sb = new StringBuilder();
    sb.append("Processing Times for Flux ").append(this.fluxId).append(":\n");
    sb.append("  [Payload]\n");
    sb.append("    Total Bytes: ").append(this.totalBytes).append("\n");
    sb.append("    Nb Element: ").append(this.nbElement).append("\n");
    sb.append("  [Server]\n");
    sb.append("    Pre:  ").append(this.serverPreProcessingTimeMs).append(" ms\n");
    sb.append("    Proc: ").append(this.serverProcessingTimeMs).append(" ms\n");
    sb.append("    Post: ").append(this.serverPostProcessingTimeMs).append(" ms\n");
    sb.append("    Total: ").append(serverTotal).append(" ms\n");

    if (pullClientTotal > 0 || this.pullClientPreProcessingTimeMs > 0 || this.pullClientProcessingTimeMs > 0
        || this.pullClientPostProcessingTimeMs > 0) {
      sb.append("  [Pull Client]\n");
      sb.append("    Pre:  ").append(this.pullClientPreProcessingTimeMs).append(" ms\n");
      sb.append("    Proc: ").append(this.pullClientProcessingTimeMs).append(" ms\n");
      sb.append("    Post: ").append(this.pullClientPostProcessingTimeMs).append(" ms\n");
      sb.append("    Total: ").append(pullClientTotal).append(" ms\n");
    }

    if (pushClientTotal > 0 || this.pushClientPreProcessingTimeMs > 0 || this.pushClientProcessingTimeMs > 0
        || this.pushClientPostProcessingTimeMs > 0) {
      sb.append("  [Push Client]\n");
      sb.append("    Pre:  ").append(this.pushClientPreProcessingTimeMs).append(" ms\n");
      sb.append("    Proc: ").append(this.pushClientProcessingTimeMs).append(" ms\n");
      sb.append("    Post: ").append(this.pushClientPostProcessingTimeMs).append(" ms\n");
      sb.append("    Total: ").append(pushClientTotal).append(" ms\n");
    }

    return sb.toString();
  }

}
