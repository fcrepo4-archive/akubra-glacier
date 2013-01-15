package org.fcrepo.akubra.glacier;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
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
public class GlacierInventoryManager extends HashMap<URI, GlacierInventoryObject> implements Serializable {
	private static final long serialVersionUID = -7742970047429355444L;
	private AmazonGlacierClient glacier;
	private String vault;
	private HashMap<URI, GlacierInventoryObject> hash;
	
	private boolean ready;
	public GlacierInventoryManager(AmazonGlacierClient glacier, String vault) {
		this.glacier = glacier;
		this.vault = vault;
		this.hash = new HashMap<URI, GlacierInventoryObject>();
		this.ready = false;
		readInventoryFromCache();
		this.ready = true;
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
		GlacierInventoryObject obj = hash.put(key, value);
		dumpInventoryToCache();
		return obj;
	}
	
	public void putAll(Map<? extends URI, ? extends GlacierInventoryObject>  t) {
		hash.putAll(t);
		dumpInventoryToCache();
	}
	
	public void remove(URI key) {
		hash.remove(key);
		dumpInventoryToCache();
	}
	
	public int size() {
		return hash.size();
	}
	
	public Collection<GlacierInventoryObject> values() {
		return hash.values();
	}
	
	public void asyncUpdateGlacierInventory() {
		final GlacierInventoryManager i = this;
		Executors.newSingleThreadExecutor().submit(new Callable<Boolean>() {
	         public Boolean call() { i.updateGlacierInventory(); return true; }
	     });
		
	}
	  
	public void updateGlacierInventory() {
		GlacierInventoryRequest request = new GlacierInventoryRequest(glacier, vault);
		
		Future<HashMap<URI, GlacierInventoryObject>> submit = Executors.newSingleThreadExecutor().submit(request);
			
			HashMap<URI, GlacierInventoryObject> old_hash = this.hash;
			try {
				this.hash = submit.get();
				if(old_hash != null) {
					for(Entry<URI, GlacierInventoryObject> entry : old_hash.entrySet()) {
						if(!this.hash.containsKey(entry.getKey()) && entry.getValue() instanceof TransientGlacierInventoryObject) {
							this.hash.put(entry.getKey(), entry.getValue());
						}
					}
				}

			
			dumpInventoryToCache();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	}
	
	private void dumpInventoryToCache() {
		if(this.ready == false) {
			return;
		}
		
		// dump our data object
		try {
			System.out.println("dumping inventory");
			ArrayList<TransientGlacierInventoryObject> c = new ArrayList<TransientGlacierInventoryObject>();
			
			for(GlacierInventoryObject e : values()) {
				System.out.println("dumping " + e.getSerializableObject().toString());
				c.add(e.getSerializableObject());
			}
			
			
			FileOutputStream f_out = new 
					FileOutputStream(getInventorySerializationFilename());
			ObjectOutputStream obj_out = new
					ObjectOutputStream (f_out);
			obj_out.writeObject( c );
		} catch (FileNotFoundException e) {
			return;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void readInventoryFromCache() {
		// Read from disk using FileInputStream

		Object obj = null;
		try {
			FileInputStream f_in = new 
				FileInputStream(getInventorySerializationFilename());
			ObjectInputStream obj_in = new ObjectInputStream (f_in);
			obj = obj_in.readObject();
		} catch (FileNotFoundException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if ( obj instanceof ArrayList<?>) {
			 
			for(Object e : ((ArrayList<?>) obj)) {
				if ( e instanceof GlacierInventoryObject ) {
					this.put(((GlacierInventoryObject)e).getBlobId(), (GlacierInventoryObject)e);
				}
			}	
		}
		
		asyncUpdateGlacierInventory();
	}
	
	private String getInventorySerializationFilename() {
		return "glacier-inventory-" + vault + ".data";
	}


}
