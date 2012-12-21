package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.LinkedList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
import com.amazonaws.services.glacier.model.DescribeJobResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class GlacierInventoryManager extends HashMap<URI, GlacierInventoryObject> {
	private static final long serialVersionUID = -7742970047429355444L;
	private AmazonGlacierClient glacier;
	private String vault;
	private HashMap<URI, GlacierInventoryObject> hash;
	
	public GlacierInventoryManager(AmazonGlacierClient glacier, String vault) {
		this.glacier = glacier;
		this.vault = vault;
		initializeBlobIdToGlacierInventoryHash();
	}
	
	public void clear() {
	}
	
	public GlacierInventoryManager clone() {
		return new GlacierInventoryManager(glacier, vault);	
	}
	
	public boolean containsKey(String key) {
		return hash.containsKey(key);
	}
	
	public boolean containsVault(GlacierInventoryObject value) {
		return hash.containsKey(value);
	}
	
	public Set<Map.Entry<URI,GlacierInventoryObject>> entrySet() {
		return hash.entrySet();
	}
	
	public GlacierInventoryObject get(URI key) {
		return hash.get(key);
	}
	
	public boolean isEmpty() {
		return hash.isEmpty();
	}
	
	public Set<URI> keySet() {
		return hash.keySet();
	}
	
	public GlacierInventoryObject put(URI key, GlacierInventoryObject value) {
		return hash.put(key, value);
	}
	
	public void putAll(Map<? extends URI, ? extends GlacierInventoryObject>  t) {
		hash.putAll(t);
	}
	
	public void remove(URI key) {
		hash.remove(key);
	}
	
	public int size() {
		return hash.size();
	}
	
	public Collection<GlacierInventoryObject> values() {
		return hash.values();
	}
  
	private void initializeBlobIdToGlacierInventoryHash() {
        InputStream is = lastAvailableInventory();
        
        HashMap<URI, GlacierInventoryObject> new_hash = new HashMap<URI, GlacierInventoryObject>();
	
        try {
        	byte[] bytes = null;
        	
        	bytes = new byte[is.available()];
        	is.read(bytes);
        	
        	String resultString = new String(bytes);
        	JSONObject json = new JSONObject(resultString);
        	JSONArray archives = json.getJSONArray("ArchiveList");
		
        	for ( int i = 0; i < archives.length(); i++ ) {
        		JSONObject j = archives.getJSONObject(i);
        		GlacierInventoryObject gio = new GlacierInventoryObject(j);
            	new_hash.put(URI.create(j.getString("ArchiveDescription")), gio);
        	}
		
        } catch (JSONException e) {
        } catch (IOException e) {
        }
	
        this.hash = new_hash;
	}
	
	private InputStream lastAvailableInventory() {
		return retrieveInventoryFromGlacier();
	}
	
	private InputStream retrieveInventoryFromGlacier() {
		JobParameters jobParameters = new JobParameters()
		.withType("inventory-retrieval");
	InitiateJobResult archiveRetrievalResult =
		glacier.initiateJob(new InitiateJobRequest()
			.withVaultName(vault)
			.withJobParameters(jobParameters));
	String jobId = archiveRetrievalResult.getJobId();

	waitForJobToComplete(jobId);
	GetJobOutputResult jobOutputResult = glacier.getJobOutput(new GetJobOutputRequest()
		.withVaultName(vault)
		.withJobId(jobId));
	
	return jobOutputResult.getBody();
	}

	private void waitForJobToComplete(String jobId) {
		while(true) {
    		DescribeJobResult result = glacier.describeJob(new DescribeJobRequest()
				.withJobId(jobId)
				.withVaultName(vault));
    		
    		if (result.getCompleted()) return;
		try {
    		Thread.sleep(1000*1);
		} catch (InterruptedException ie) {
			throw new AmazonClientException("Inventory download interrupted", ie);
		}
		}
		
		
	}

}
