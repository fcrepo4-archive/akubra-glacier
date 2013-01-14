package org.fcrepo.akubra.glacier;

import java.net.URI;
import java.util.Date;

public class TransientGlacierInventoryObject extends GlacierInventoryObject {

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
}
