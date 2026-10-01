package fr.jdiot.dev.flux.core;

import fr.jdiot.dev.flux.core.ack.Acknowledgement;
import fr.jdiot.dev.flux.core.ack.Status;

public class AcknowledgementUtils {

  public static Acknowledgement success(final String fluxId) {
    return new Acknowledgement(fluxId, Status.SUCCESS, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);
  }

  public static Acknowledgement failed(final String fluxId) {
    return new Acknowledgement(fluxId, Status.FAILED, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);
  }

  public static Acknowledgement failed(final String fluxId, final String reason) {
    return new Acknowledgement(fluxId, Status.FAILED, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, reason);
  }

  public static Acknowledgement partial(final String fluxId) {
    return new Acknowledgement(fluxId, Status.PARTIAL, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);
  }

  public static String pritableReport(final Acknowledgement ack) {
    final long serverTotal = ack.getServerPreProcessingTimeMs() + ack.getServerProcessingTimeMs()
        + ack.getServerPostProcessingTimeMs();
    final long pullClientTotal = ack.getPullClientPreProcessingTimeMs() + ack.getPullClientProcessingTimeMs()
        + ack.getPullClientPostProcessingTimeMs();
    final long pushClientTotal = ack.getPushClientPreProcessingTimeMs() + ack.getPushClientProcessingTimeMs()
        + ack.getPushClientPostProcessingTimeMs();

    final StringBuilder sb = new StringBuilder();
    sb.append("Processing Times for Flux ").append(ack.getFluxId()).append(":\n");
    sb.append("  [Payload]\n");
    sb.append("    Total Bytes: ").append(ack.getTotalBytes()).append("\n");
    sb.append("    Nb Element: ").append(ack.getNbElement()).append("\n");
    sb.append("  [Server]\n");
    sb.append("    Pre:  ").append(ack.getServerPreProcessingTimeMs()).append(" ms\n");
    sb.append("    Proc: ").append(ack.getServerProcessingTimeMs()).append(" ms\n");
    sb.append("    Post: ").append(ack.getServerPostProcessingTimeMs()).append(" ms\n");
    sb.append("    Total: ").append(serverTotal).append(" ms\n");

    if (pullClientTotal > 0 || ack.getPullClientPreProcessingTimeMs() > 0 || ack.getPullClientProcessingTimeMs() > 0
        || ack.getPullClientPostProcessingTimeMs() > 0) {
      sb.append("  [Pull Client]\n");
      sb.append("    Pre:  ").append(ack.getPullClientPreProcessingTimeMs()).append(" ms\n");
      sb.append("    Proc: ").append(ack.getPullClientProcessingTimeMs()).append(" ms\n");
      sb.append("    Post: ").append(ack.getPullClientPostProcessingTimeMs()).append(" ms\n");
      sb.append("    Total: ").append(pullClientTotal).append(" ms\n");
    }

    if (pushClientTotal > 0 || ack.getPushClientPreProcessingTimeMs() > 0 || ack.getPushClientProcessingTimeMs() > 0
        || ack.getPushClientPostProcessingTimeMs() > 0) {
      sb.append("  [Push Client]\n");
      sb.append("    Pre:  ").append(ack.getPushClientPreProcessingTimeMs()).append(" ms\n");
      sb.append("    Proc: ").append(ack.getPushClientProcessingTimeMs()).append(" ms\n");
      sb.append("    Post: ").append(ack.getPushClientPostProcessingTimeMs()).append(" ms\n");
      sb.append("    Total: ").append(pushClientTotal).append(" ms\n");
    }

    return sb.toString();
  }
}
