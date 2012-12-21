package org.fcrepo.akubra.glacier;

import java.net.URI;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class GlacierInventoryObject {
	private AmazonGlacierClient glacier;
	private JSONObject properties;
	private String archiveId;

	public GlacierInventoryObject(AmazonGlacierClient glacier, JSONObject properties) {
		this.glacier = glacier;
		this.properties = properties;
	}
	
	public GlacierInventoryObject(AmazonGlacierClient glacier, String archiveId) {
		this.archiveId = archiveId;
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
	
	public URI getBlobId() {
		try {
			return URI.create(properties.getString("ArchiveDescription"));
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
}
