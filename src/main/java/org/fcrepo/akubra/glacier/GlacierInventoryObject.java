package org.fcrepo.akubra.glacier;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class GlacierInventoryObject {
	private JSONObject properties;
	private String archiveId;
	private Long size;
	private URI blobId;

	public GlacierInventoryObject(JSONObject properties) {
		this.properties = properties;
	}
	
	public GlacierInventoryObject(URI blobId, String archiveId, Long size) {
		this.blobId = blobId;
		this.archiveId = archiveId;
		this.size = size;
	}

	public String getArchiveId() {
		if(this.archiveId != null) {
			return this.archiveId;
		}
		
		try {
			return properties.getString("ArchiveId");
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public Date getCreationDate() {

		try {
			String creationDate = properties.getString("CreationDate");
			return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse(creationDate);
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public long getSize() {
		if(this.size != null) {
			return this.size;
		}
		
		try {
			return properties.getLong("Size");
		} catch (JSONException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	public String getArchiveDescription() {
		try {
			return properties.getString("ArchiveDescription");
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public URI getBlobId() {
		if(this.blobId != null) {
			return this.blobId;
		}
		
		return URI.create(getArchiveDescription());
	}
}
