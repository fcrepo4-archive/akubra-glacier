package org.fcrepo.akubra.glacier;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;
import org.akubraproject.impl.StreamManager;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.CreateVaultRequest;

public class GlacierBlobStore extends AbstractBlobStore {

	private final AmazonGlacierClient glacier;
	private final String vault;
	private final StreamManager manager = new StreamManager();
	private final GlacierInventoryManager inventory_manager;

	private BlobStore m_synchronousStore;

    public static String STORAGE_HINT_VALUE_GLACIER = "aws-glacier";
    public static String STORAGE_HINT_KEY = "org.fcrepo.futures.store";

	public GlacierBlobStore(URI id, AmazonGlacierClient glacier, String vault) {
		super(id);
		this.glacier = glacier;
		this.vault = vault;

		createInitialVault();
		this.inventory_manager = new GlacierInventoryManager(glacier, vault);
	}

	public void setSynchronousStore(BlobStore synchronousStore) {
	    m_synchronousStore = synchronousStore;
	}

	public BlobStoreConnection openConnection(Transaction tx,
			Map<String, String> hints) throws UnsupportedOperationException,
			IOException {
	    if (m_synchronousStore != null) {
	        if (hints != null && STORAGE_HINT_VALUE_GLACIER.equals(hints.get(STORAGE_HINT_KEY))){
	            return new GlacierBlobStoreConnection(this, glacier, vault, manager);
	        }
	        else {
	            return m_synchronousStore.openConnection(tx, hints);
	        }
	    }
	    else {
	        return new GlacierBlobStoreConnection(this, glacier, vault, manager);
	    }
	}


	private void createInitialVault() {

		CreateVaultRequest request = new CreateVaultRequest()
			.withAccountId("-")
			.withVaultName(this.vault);


	    glacier.createVault(request);
	}

	public GlacierInventoryManager getGlacierInventoryManager() {
		return inventory_manager;
	}

}
