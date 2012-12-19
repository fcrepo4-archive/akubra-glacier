package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;

public class GlacierBlobStore extends AbstractBlobStore {

	protected GlacierBlobStore(URI id) {
		super(id);
		// TODO Auto-generated constructor stub
	}

	public BlobStoreConnection openConnection(Transaction arg0,
			Map<String, String> arg1) throws UnsupportedOperationException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
