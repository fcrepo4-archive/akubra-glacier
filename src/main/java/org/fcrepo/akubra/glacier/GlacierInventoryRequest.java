package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.Callable;

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


public class GlacierInventoryRequest implements Callable<HashMap<URI, GlacierInventoryObject>> {
	private AmazonGlacierClient glacier;
	private String vault;
	
	public GlacierInventoryRequest(AmazonGlacierClient glacier, String vault) {
		this.glacier = glacier;
		this.vault = vault;
	}
	
	public HashMap<URI, GlacierInventoryObject> call() {
		InputStream is = retrieveInventoryFromGlacier();
	    
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
	    
	    return new_hash;
	}
	
	private InputStream retrieveInventoryFromGlacier() {
		JobParameters jobParameters = new JobParameters()
		.withType("inventory-retrieval");
		
		InitiateJobResult archiveRetrievalResult = glacier.initiateJob(
			new InitiateJobRequest()
				.withVaultName(vault)
				.withJobParameters(jobParameters));
		
		String jobId = archiveRetrievalResult.getJobId();
	
		waitForJobToComplete(jobId);
	
		GetJobOutputResult jobOutputResult = glacier.getJobOutput(
				new GetJobOutputRequest()
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