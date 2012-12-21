package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.DuplicateBlobException;
import org.akubraproject.MissingBlobException;
import org.akubraproject.impl.AbstractBlob;
import org.akubraproject.impl.StreamManager;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;

public class GlacierBlob extends AbstractBlob {

	private URI blobId;
	private StreamManager manager;
	private GlacierBlobStoreConnection connection;
	private AmazonGlacierClient client;

	public GlacierBlob(GlacierBlobStoreConnection connection, URI blobId, StreamManager manager) {
		super(connection, blobId);
		
		this.connection = connection;
		this.client = connection.getGlacierClient();
		this.blobId = blobId;
		this.manager = manager;
	}
	
	public void delete() throws IOException {
		client.deleteArchive(new DeleteArchiveRequest()
		.withVaultName(getVault())
		.withArchiveId(getArchiveId()));
		
		connection.getGlacierInventoryManager().remove(blobId);
	}

	public boolean exists() throws IOException {
		return getArchiveId() != null;
	}

	public long getSize() throws IOException, MissingBlobException {

	    if (!exists())
	      throw new MissingBlobException(getId());
	    
		GlacierInventoryObject obj = getInventoryObject();
		return obj.getSize();
	}

	public Blob moveTo(URI blobId, Map<String, String> hints)
			throws DuplicateBlobException, IOException, MissingBlobException,
			NullPointerException, IllegalArgumentException {
		ensureOpen();
		
	    if (!exists())
	        throw new MissingBlobException(getId());
		
	    GlacierBlob dest = (GlacierBlob) getConnection().getBlob(blobId, hints);

	    if (dest.exists())
	        throw new DuplicateBlobException(blobId);

	    OutputStream os = dest.openOutputStream(this.getSize(), false);
	    InputStream is = this.openInputStream();
	    
	    byte[] buffer = new byte[1024*1024]; // Adjust if you want
	    int bytesRead;
	    while ((bytesRead = is.read(buffer)) != -1)
	    {
	        os.write(buffer, 0, bytesRead);
	    }
	    
	    this.delete();
	    
	    return dest;
	}

	public InputStream openInputStream() throws IOException,
			MissingBlobException {
		ensureOpen();

	    if (!exists())
	      throw new MissingBlobException(getId());
	    
		InputStream gis = new GlacierInputStream(client, getVault(), getArchiveId());
		
	    return manager.manageInputStream(getConnection(), gis);
	}

	public OutputStream openOutputStream(long expectedSize, boolean overwrite)
			throws IOException, DuplicateBlobException {
		ensureOpen();

	    if (!overwrite && exists())
	      throw new DuplicateBlobException(getId());
	    
		OutputStream os = new GlacierMultipartBufferedOutputStream(connection, getVault(), blobId);
	    return manager.manageOutputStream(getConnection(), os);
	}
	
	private String getVault() {
		return connection.getVault();
	}
	
	private GlacierInventoryObject getInventoryObject() {
		GlacierInventoryManager im = connection.getGlacierInventoryManager();
		
		return im.get(blobId);
	}
	
	private String getArchiveId() {
		GlacierInventoryObject obj = getInventoryObject();
		if(obj != null) {
			return obj.getArchiveId();
		} else {
			return null;
		}
	}

}
