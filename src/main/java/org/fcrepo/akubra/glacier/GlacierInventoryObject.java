package org.fcrepo.akubra.glacier;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class GlacierInventoryObject {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1123168261007582925L;
	@Override
	public String toString() {
		return "GlacierInventoryObject [getArchiveId()=" + getArchiveId()
				+ ", getCreationDate()=" + getCreationDate() + ", getSize()="
				+ getSize() + ", getBlobId()=" + getBlobId() + "]";
	}

	private JSONObject properties;
	
	public GlacierInventoryObject() {
		
	}
	
	public GlacierInventoryObject(JSONObject properties) {
		this.properties = properties;
	}

	public String getArchiveId() {
		
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
		return URI.create(getArchiveDescription());
	}
	
	public TransientGlacierInventoryObject getSerializableObject() {
		return new TransientGlacierInventoryObject(getBlobId(), getArchiveId(), getSize());
	}

}
