package org.fcrepo.akubra.glacier;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.DuplicateBlobException;
import org.akubraproject.MissingBlobException;
import org.akubraproject.impl.AbstractBlob;
import org.akubraproject.impl.StreamManager;

public class GlacierBlob extends AbstractBlob {

	private URI blobId;
	private StreamManager manager;

	public GlacierBlob(GlacierBlobStoreConnection connection, URI blobId, StreamManager manager) {
		super(connection, blobId);
		
		this.blobId = blobId;
		this.manager = manager;
		// TODO Auto-generated constructor stub
	}

	public void delete() throws IOException {
		// TODO Auto-generated method stub

	}

	public boolean exists() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	public long getSize() throws IOException, MissingBlobException {
		// TODO Auto-generated method stub
		return 0;
	}

	public Blob moveTo(URI arg0, Map<String, String> arg1)
			throws DuplicateBlobException, IOException, MissingBlobException,
			NullPointerException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	public InputStream openInputStream() throws IOException,
			MissingBlobException {
		// TODO Auto-generated method stub
		return null;
	}

	public OutputStream openOutputStream(long arg0, boolean arg1)
			throws IOException, DuplicateBlobException {
	    return manager.manageOutputStream(getConnection(), new GlacierOutputStream(connection, blobId));
	}

}
