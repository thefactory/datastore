package com.thefactory.datastore;

import java.io.IOException;
import java.io.Closeable;

public interface FileSystem {
    // Open a named resource for writing
    public DatastoreChannel create(String name);

    // Open a named resource for reading
    public DatastoreChannel open(String name);

    // Open a named resource for appending
    public DatastoreChannel append(String name);

    // Check if a resource exists
    boolean exists(String name);

    // Remove/delete a resource 
    void remove(String name);

    // Rename a resource
    void rename(String oldName, String newName);

    // Create full path to a resource
    void mkdirs(String name);

    // Get the size in bytes for a resource
    long size(String name);

    // Lock a resource
    Closeable lock(String name) throws IOException;

    // Return a list with all resources names
    String[] list(String dir);
}
