package com.thefactory.datastore;

import java.lang.Exception;

public class KeyNotFoundException extends Exception {
    public KeyNotFoundException(String message) {
        super(message);
    }
} 