package org.fcrepo.akubra.glacier;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazonaws.services.glacier.AmazonGlacierClient;
/***
 * The GlacierInventoryManager maps Akubra BlobIds to Glacier metadata
 * 
 * @author cabeer
 */
public class GlacierInventoryManager extends HashMap<URI, GlacierInventoryObject> {
	private static final long serialVersionUID = -7742970047429355444L;
	private AmazonGlacierClient glacier;
	private String vault;
	private HashMap<URI, GlacierInventoryObject> hash;
	
	public GlacierInventoryManager(AmazonGlacierClient glacier, String vault) {
		this.glacier = glacier;
		this.vault = vault;
		updateGlacierInventory();
	}
	
	public void clear() {
	}
	
	public GlacierInventoryManager clone() {
		return new GlacierInventoryManager(glacier, vault);	
	}
	
	public boolean containsKey(String key) {
		return hash.containsKey(key);
	}
	
	public boolean containsValue(GlacierInventoryObject value) {
		return hash.containsValue(value);
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
	  
	private void updateGlacierInventory() {
		GlacierInventoryRequest request = new GlacierInventoryRequest(glacier, vault);
		
		ExecutorService executor = Executors.newFixedThreadPool(1);
			
		Future<HashMap<URI, GlacierInventoryObject>> submit = executor.submit(request);
			
		try {
			HashMap<URI, GlacierInventoryObject> old_hash = this.hash;
			this.hash = submit.get();
			
			if(old_hash != null) {
				for(Entry<URI, GlacierInventoryObject> entry : old_hash.entrySet()) {
					if(!this.hash.containsKey(entry.getKey()) && entry.getValue() instanceof TransientGlacierInventoryObject) {
						this.hash.put(entry.getKey(), entry.getValue());
					}
				}
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

}
