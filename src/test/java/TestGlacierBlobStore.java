import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.fcrepo.akubra.glacier.GlacierBlobStore;
import org.fcrepo.akubra.glacier.GlacierBlobStoreConnection;
import org.junit.Test;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;


public class TestGlacierBlobStore {

	@Test
	public void test() throws IOException {
		PropertiesCredentials credentials = new PropertiesCredentials(TestGlacierBlobStore.class.getResourceAsStream("AwsCredentials.properties"));
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint("http://localhost:3000/");
		
		GlacierBlobStore store = new GlacierBlobStore(URI.create("urn:my-aws-blobstore"), client, "test-akubra-vault");
		
		BlobStoreConnection connection = store.openConnection(null, null);
		
		assertFalse(connection.listBlobIds(null).hasNext());
		
		Blob b = connection.getBlob(URI.create("info:fedora/object:pid/dsID.1"), null);
		
		OutputStream os = b.openOutputStream(0, false);
		
		os.write("asdf".getBytes(), 0, 4);
		os.close();
		assertTrue(connection.listBlobIds(null).hasNext());
		

		Blob b2 = connection.getBlob(URI.create("info:fedora/object:pid/dsID.1"), null);
		assertTrue(b2.exists());
		InputStream is = b2.openInputStream();
		
		byte[] content = new byte[4];
		is.read(content);
		
		String cs = new String(content);
		assertEquals("asdf", cs);
		
		b2.delete();
		
		
		
	}

}
