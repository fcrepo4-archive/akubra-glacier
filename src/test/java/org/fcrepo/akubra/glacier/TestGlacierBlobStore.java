package org.fcrepo.akubra.glacier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.fcrepo.akubra.glacier.fcrepo.GlacierStorageHintProvider;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;

@RunWith(MockitoJUnitRunner.class)
public class TestGlacierBlobStore {

    @Mock
    private DigitalObject testObj;

    @Mock
    private Datastream testDS;

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

    @Test
    public void testDelegation() throws IOException {
        PropertiesCredentials credentials = new PropertiesCredentials(TestGlacierBlobStore.class.getResourceAsStream("AwsCredentials.properties"));
        AmazonGlacierClient client = new AmazonGlacierClient(credentials);
        client.setEndpoint("http://localhost:3000/");

        GlacierBlobStore store = new GlacierBlobStore(URI.create("urn:my-aws-blobstore"), client, "test-akubra-vault");

        store.setSynchronousStore(new MockBlobStore(URI.create("urn:my-synch-blobstore")));

        BlobStoreConnection connection = store.openConnection(null, null);

        assertTrue(connection instanceof MockBlobStoreConnection);

        when(testObj.getPid()).thenReturn("object:pid");

        testDS.DatastreamID = "dsID.1";
        testDS.DatastreamAltIDs = new String[]{"info:fedora/object:pid/dsID?store=aws-glacier"};
        when(testObj.datastreams("dsID.1")).thenReturn(Arrays.asList(new Datastream[]{testDS}));

        GlacierStorageHintProvider hints = new GlacierStorageHintProvider();

        connection = store.openConnection(null, hints.getHintsForAboutToBeStoredDatastream(testObj, "dsID"));

        assertTrue(connection instanceof GlacierBlobStoreConnection);

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
