package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;


public class MockBlobStoreConnection extends AbstractBlobStoreConnection {

    protected MockBlobStoreConnection(BlobStore owner) {
        super(owner);
    }

    public Blob getBlob(URI blobId, Map<String, String> hints)
            throws IOException, UnsupportedIdException,
            UnsupportedOperationException {
        return null;
    }

    public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
        return null;
    }

    public void sync() throws IOException, UnsupportedOperationException {

    }

}
