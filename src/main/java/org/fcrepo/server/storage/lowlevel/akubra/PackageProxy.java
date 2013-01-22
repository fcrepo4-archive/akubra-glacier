package org.fcrepo.server.storage.lowlevel.akubra;

import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;

import org.akubraproject.BlobStoreConnection;
import org.fcrepo.server.storage.lowlevel.akubra.AkubraLowlevelStorage.ConnectionClosingInputStream;
import org.fcrepo.server.storage.lowlevel.akubra.AkubraLowlevelStorage.ConnectionClosingKeyIterator;

public class PackageProxy {
	
	public static ConnectionClosingInputStream wrapStream(BlobStoreConnection blobStore, InputStream input) {
		return new ConnectionClosingInputStream(blobStore, input);
	}
    public static ConnectionClosingKeyIterator getKeyIterator(BlobStoreConnection connection,
            Iterator<URI> blobIds) {
        return new ConnectionClosingKeyIterator(connection, blobIds);
    }
}
