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
			.withVaultName(this.vault);
		

	    getGlacierClient().createVault(request);
	}

	public Blob getBlob(URI blobId, Map<String, String> arg1) throws IOException,
			UnsupportedIdException, UnsupportedOperationException {
		Blob b = new GlacierBlob(this, blobId, streamManager);
		return b;
	}

	public Iterator<URI> listBlobIds(String arg0) throws IOException {
		LinkedList<URI> list = new LinkedList<URI>();
		GlacierInventoryManager g = getGlacierInventoryManager();
	
		Iterator<GlacierInventoryObject> it = g.values().iterator();
		
		while(it.hasNext()) {
			GlacierInventoryObject j = it.next();
			list.add(j.getBlobId());
		}
		
		return list.iterator();
	}

	public void sync() throws IOException, UnsupportedOperationException {
		// TODO Auto-generated method stub
	}

	public AmazonGlacierClient getGlacierClient() {
		return this.glacier;
	}

	public String getVault() {
		return this.vault;
	}
	
	public GlacierInventoryManager getGlacierInventoryManager() {
		return new GlacierInventoryManager(glacier, vault);	
	}
	

}
