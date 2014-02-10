package com.thefactory.datastore;

import java.io.IOException;
import java.io.Closeable;
import java.util.Collection;

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

    // Create full directory path to a resource
    void mkdirs(String name);

    // Get the size in bytes for a resource
    long size(String name);

    // Lock a resource
    Closeable lock(String name) throws IOException;

    // Store a list of strings as a named resource
    void storeList(Collection<String> items, String name) throws IOException;

    // Load a list of strings from a named resource
    Collection<String> loadList(String name) throws IOException;

}
