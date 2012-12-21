package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;
import org.akubraproject.impl.StreamManager;

import com.amazonaws.services.glacier.AmazonGlacierClient;

public class GlacierBlobStore extends AbstractBlobStore {

	private AmazonGlacierClient glacier;
	private String vault;
	private final StreamManager manager = new StreamManager();

	public GlacierBlobStore(URI id, AmazonGlacierClient glacier, String vault) {
		super(id);
		this.glacier = glacier;
		this.vault = vault;
	}

	public BlobStoreConnection openConnection(Transaction arg0,
			Map<String, String> arg1) throws UnsupportedOperationException,
			IOException {
	    return new GlacierBlobStoreConnection(this, glacier, vault, manager);
	}

}
