package org.fcrepo.akubra.glacier;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.junit.Test;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;

public class TestGlacierInventoryManager {

	@Test
	public void testInventorySerialization() throws IOException {
		PropertiesCredentials credentials = new PropertiesCredentials(TestGlacierInventoryManager.class.getResourceAsStream("AwsCredentials.properties"));
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		GlacierInventoryManager m = new GlacierInventoryManager(client, "serialization-test-vault");
		m.put(URI.create("info:fedora/abc"), new TransientGlacierInventoryObject(URI.create("info:fedora/abc"), "asdf", (long)0));
		m.put(URI.create("info:fedora/def"), new TransientGlacierInventoryObject(URI.create("info:fedora/def"), "1234", (long)0));
		m.put(URI.create("info:fedora/ghi"), new TransientGlacierInventoryObject(URI.create("info:fedora/ghi"), "4321", (long)0));
		

		GlacierInventoryManager m1 = new GlacierInventoryManager(client, "serialization-test-vault");
		
		assertEquals(3, m1.values().toArray().length);

	}
	

}
