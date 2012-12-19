package org.fcrepo.akubra.glacier;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
import com.amazonaws.services.glacier.model.DescribeJobResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.transfer.JobStatusMonitor;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class GlacierInputStream extends InputStream {
	private AmazonGlacierClient client;
	private URI blobId;
	private String vault;
	private BufferedInputStream input;
	
	public GlacierInputStream(AmazonGlacierClient client, String vault, URI blobId) {
		this.client = client;
		this.blobId = blobId;
		this.vault = vault;
	}
	
	@Override
	public int read() throws IOException {
		return getInputStream().read();
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		return getInputStream().read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return getInputStream().read(b, off, len);
	}
	
	public int available() {
		try {
			return getInputStream().available();
		} catch (IOException e) {
			return 0;
		}
	}
	
	
	private InputStream getInputStream() {

		if(this.input == null) {
			GetJobOutputResult jobOutputResult = initiateDownloadJob();
			this.input = new BufferedInputStream(jobOutputResult.getBody());
		}

		return input;
	}

	private GetJobOutputResult initiateDownloadJob() {
    		JobParameters jobParameters = new JobParameters()
    			.withArchiveId(this.getArchiveId())
    			.withType("archive-retrieval");
    		InitiateJobResult archiveRetrievalResult =
    			client.initiateJob(new InitiateJobRequest()
    				.withVaultName(vault)
    				.withJobParameters(jobParameters));
    		String jobId = archiveRetrievalResult.getJobId();

    		waitForJobToComplete(jobId);
    		return client.getJobOutput(new GetJobOutputRequest()
    			.withVaultName(vault)
    			.withJobId(jobId));
	}

	private void waitForJobToComplete(String jobId) {
		while(true) {
    		DescribeJobResult result = client.describeJob(new DescribeJobRequest()
				.withJobId(jobId)
				.withVaultName(vault));
    		
    		if (result.getCompleted()) return;
		try {
    		Thread.sleep(1000*1);
		} catch (InterruptedException ie) {
			throw new AmazonClientException("Archive download interrupted", ie);
		}
		}
		
		
	}

	private String getArchiveId() {

		JobParameters jobParameters = new JobParameters()
			.withType("inventory-retrieval");
		InitiateJobResult archiveRetrievalResult =
			client.initiateJob(new InitiateJobRequest()
				.withVaultName(vault)
				.withJobParameters(jobParameters));
		String jobId = archiveRetrievalResult.getJobId();

		waitForJobToComplete(jobId);
		GetJobOutputResult jobOutputResult = client.getJobOutput(new GetJobOutputRequest()
			.withVaultName(vault)
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
				String description = j.getString("ArchiveDescription");
				if(description.equals(blobId.toString())) {
					return j.getString("ArchiveId");
				}
				
			}
			
		} catch (JSONException e) {
		}
		
		return "";
	}

}
