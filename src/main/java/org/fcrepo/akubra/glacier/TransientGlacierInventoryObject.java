package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Date;

public class TransientGlacierInventoryObject extends GlacierInventoryObject implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7597557003012939729L;
	private String archiveId;
	private Long size;
	private URI blobId;
	private Date creation_date;

	public TransientGlacierInventoryObject(URI blobId, String archiveId, Long size) {
		super();
		this.blobId = blobId;
		this.archiveId = archiveId;
		this.size = size;
		this.creation_date = new Date();
	}
	
	public String getArchiveId() {
		return this.archiveId;
	}
	
	public Date getCreationDate() {
		return this.creation_date;
	}
	
	public long getSize() {
		return this.size;
	}
	
	public String getArchiveDescription() {
		return this.blobId.toString();
	}
	
	public URI getBlobId() {
		return this.blobId;
	}
	
	public TransientGlacierInventoryObject getSerializableObject() {
		return this;
	}
	
	private void writeObject(java.io.ObjectOutputStream stream) throws IOException {
		stream.defaultWriteObject();
	}
	
	private void readObject(java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
	}
}
