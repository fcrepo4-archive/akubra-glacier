package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;


public class MockBlobStore extends AbstractBlobStore {

    protected MockBlobStore(URI id) {
        super(id);
    }

    public BlobStoreConnection openConnection(Transaction tx,
            Map<String, String> hints) throws UnsupportedOperationException,
            IOException {
        return new MockBlobStoreConnection(null);
    }

}
