package fr.jdiot.dev.flux.exception;

/**
 * Domain-specific runtime exception for FLUX API.
 */
public class FluxException extends RuntimeException {

  public FluxException(final String message) {
    super(message);
  }

  public FluxException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
