package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.akubraproject.impl.StreamManager;

public class GlacierBlobStoreConnection extends AbstractBlobStoreConnection {

	public GlacierBlobStoreConnection(BlobStore owner) {
		super(owner);
		// TODO Auto-generated constructor stub
	}

	public GlacierBlobStoreConnection(BlobStore owner,
			StreamManager streamManager) {
		super(owner, streamManager);
		// TODO Auto-generated constructor stub
	}

	public Blob getBlob(URI arg0, Map<String, String> arg1) throws IOException,
			UnsupportedIdException, UnsupportedOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	public Iterator<URI> listBlobIds(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public void sync() throws IOException, UnsupportedOperationException {
		// TODO Auto-generated method stub

	}

}
