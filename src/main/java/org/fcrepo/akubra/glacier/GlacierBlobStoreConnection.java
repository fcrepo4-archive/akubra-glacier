package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.akubraproject.impl.StreamManager;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.CreateVaultRequest;
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

public class GlacierBlobStoreConnection extends AbstractBlobStoreConnection {

	public GlacierBlobStoreConnection(BlobStore owner) {
		super(owner);
		createInitialVault();
	}

	public GlacierBlobStoreConnection(BlobStore owner,
			StreamManager streamManager) {
		super(owner, streamManager);
		
		createInitialVault();
	}
	
	private void createInitialVault() {

		CreateVaultRequest request = new CreateVaultRequest()
			.withAccountId("-")
			.withVaultName(getVault());
		

	    getClient().createVault(request);
	}

	public Blob getBlob(URI blobId, Map<String, String> arg1) throws IOException,
			UnsupportedIdException, UnsupportedOperationException {
		Blob b =  new GlacierBlob(this, blobId, streamManager);
		return b;
	}

	public Iterator<URI> listBlobIds(String arg0) throws IOException {
		LinkedList<URI> list = new LinkedList<URI>();
		JobParameters jobParameters = new JobParameters()
		.withType("inventory-retrieval");
	InitiateJobResult archiveRetrievalResult =
		getClient().initiateJob(new InitiateJobRequest()
			.withVaultName(getVault())
			.withJobParameters(jobParameters));
	String jobId = archiveRetrievalResult.getJobId();

	waitForJobToComplete(jobId);
	GetJobOutputResult jobOutputResult = getClient().getJobOutput(new GetJobOutputRequest()
		.withVaultName(getVault())
		.withJobId(jobId));
	
	InputStream is = jobOutputResult.getBody();
	
	try {
		byte[] bytes = null;
		try {
			bytes = new byte[is.available()];
			is.read(bytes);
		} catch (IOException e) {
		}
		String resultString = new String(bytes);
		JSONObject json = new JSONObject(resultString);
		JSONArray archives = json.getJSONArray("ArchiveList");
		
		for ( int i = 0; i < archives.length(); i++ ) {
			JSONObject j = archives.getJSONObject(i);
			list.add(URI.create(j.getString("ArchiveId")));
		}
		
	} catch (JSONException e) {
	}
	
	return list.iterator();
	}

	public void sync() throws IOException, UnsupportedOperationException {
		// TODO Auto-generated method stub
	}

	public AmazonGlacierClient getClient() {
		AmazonGlacierClient client = new AmazonGlacierClient(getAWSPropertiesCredentials());
		client.setEndpoint("http://localhost:3000/");
		
		return client;
	
	}

	public String getVault() {
		// TODO Auto-generated method stub
		return "akubra-glacier-vault";
	}
	
	private PropertiesCredentials getAWSPropertiesCredentials() {
	  try {
		return new PropertiesCredentials(GlacierBlobStoreConnection.class.getResourceAsStream("AwsCredentials.properties"));
	} catch (IOException e) {
		e.printStackTrace();
		return null;	
	}
	}
	
	private void waitForJobToComplete(String jobId) {
		while(true) {
    		DescribeJobResult result = getClient().describeJob(new DescribeJobRequest()
				.withJobId(jobId)
				.withVaultName(getVault()));
    		
    		if (result.getCompleted()) return;
		try {
    		Thread.sleep(1000*1);
		} catch (InterruptedException ie) {
			throw new AmazonClientException("Archive download interrupted", ie);
		}
		}
		
		
	}
	
	

}
