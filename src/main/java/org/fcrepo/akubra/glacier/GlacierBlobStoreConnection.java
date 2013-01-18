package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.akubraproject.impl.StreamManager;

import com.amazonaws.services.glacier.AmazonGlacierClient;

public class GlacierBlobStoreConnection extends AbstractBlobStoreConnection {

	private final AmazonGlacierClient glacier;
	private final String vault;

	public GlacierBlobStoreConnection(BlobStore owner, AmazonGlacierClient glacier, String vault,
			StreamManager streamManager) {
		super(owner, streamManager);
		this.glacier = glacier;
		this.vault = vault;

	}


	public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException,
			UnsupportedIdException, UnsupportedOperationException {
		Blob b = new GlacierBlob(this, blobId, streamManager);
		return b;
	}

	public Iterator<URI> listBlobIds(String arg0) throws IOException {
		LinkedList<URI> list = new LinkedList<URI>();
		GlacierInventoryManager g = getGlacierInventoryManager();

		Iterator<GlacierInventoryObject> it = g.values().iterator();

		while(it.hasNext()) {
			GlacierInventoryObject j = it.next();
			list.add(j.getBlobId());
		}

		return list.iterator();
	}

	public void sync() throws IOException, UnsupportedOperationException {
		// TODO Auto-generated method stub
	}

	public AmazonGlacierClient getGlacierClient() {
		return this.glacier;
	}

	public String getVault() {
		return this.vault;
	}

	public GlacierInventoryManager getGlacierInventoryManager() {
		return ((GlacierBlobStore)this.owner).getGlacierInventoryManager();
	}


}
