package org.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    public static void handle(Exception e, String context) {
        logger.error("Exception in {}: {}", context, e.getMessage(), e);
        throw new RuntimeException(e);
    }
}
