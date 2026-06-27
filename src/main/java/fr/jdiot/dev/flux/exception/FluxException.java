package fr.jdiot.dev.flux.exception;

/**
 * Domain-specific runtime exception for FLUX API.
 */
public class FluxException extends RuntimeException {
    
    public FluxException(String message) {
        super(message);
    }
    
    public FluxException(String message, Throwable cause) {
        super(message, cause);
    }
}
