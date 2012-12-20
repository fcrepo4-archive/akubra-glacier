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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
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

public class GlacierBlob extends AbstractBlob {

	private URI blobId;
	private StreamManager manager;
	private GlacierBlobStoreConnection connection;
	private AmazonGlacierClient client;

	public GlacierBlob(GlacierBlobStoreConnection connection, URI blobId, StreamManager manager) {
		super(connection, blobId);
		
		this.connection = connection;
		this.client = connection.getClient();
		this.blobId = blobId;
		this.manager = manager;
	}
	
	public void delete() throws IOException {
		client.deleteArchive(new DeleteArchiveRequest()
		.withVaultName(getVault())
		.withArchiveId(getArchiveId()));
	}

	public boolean exists() throws IOException {
		return getArchiveId() != "";
	}

	public long getSize() throws IOException, MissingBlobException {
		// TODO Auto-generated method stub
		return 0;
	}

	public Blob moveTo(URI arg0, Map<String, String> arg1)
			throws DuplicateBlobException, IOException, MissingBlobException,
			NullPointerException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	public InputStream openInputStream() throws IOException,
			MissingBlobException {
		ensureOpen();
		
		InputStream gis = new GlacierInputStream(client, getVault(), getArchiveId());
		
	    return manager.manageInputStream(getConnection(), gis);
	}

	public OutputStream openOutputStream(long arg0, boolean arg1)
			throws IOException, DuplicateBlobException {
		ensureOpen();
		
		OutputStream os = new GlacierMultipartBufferedOutputStream(client, getVault(), blobId);
	    return manager.manageOutputStream(getConnection(), os);
	}
	
	private String getVault() {
		return connection.getVault();
	}
	
	private String getArchiveId() {

		JobParameters jobParameters = new JobParameters()
			.withType("inventory-retrieval");
		InitiateJobResult archiveRetrievalResult =
			client.initiateJob(new InitiateJobRequest()
				.withVaultName(getVault())
				.withJobParameters(jobParameters));
		String jobId = archiveRetrievalResult.getJobId();

		waitForJobToComplete(jobId);
		GetJobOutputResult jobOutputResult = client.getJobOutput(new GetJobOutputRequest()
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
				String description = j.getString("ArchiveDescription");
				if(description.equals(blobId.toString())) {
					return j.getString("ArchiveId");
				}
				
			}
			
		} catch (JSONException e) {
		}
		
		return "";
	}
	

	private void waitForJobToComplete(String jobId) {
		while(true) {
    		DescribeJobResult result = client.describeJob(new DescribeJobRequest()
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
