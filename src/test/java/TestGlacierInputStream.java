

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Random;

import org.fcrepo.akubra.glacier.GlacierInputStream;
import org.fcrepo.akubra.glacier.GlacierMultipartBufferedOutputStream;
import org.junit.Test;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.CreateVaultRequest;
import com.amazonaws.services.glacier.model.CreateVaultResult;

public class TestGlacierInputStream {

	@Test
	public void test() throws IOException {
		PropertiesCredentials credentials = new PropertiesCredentials(TestGlacierInputStream.class.getResourceAsStream("AwsCredentials.properties"));
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint("http://localhost:3000/");
	
		CreateVaultRequest request = new CreateVaultRequest()
			.withAccountId("-")
			.withVaultName("akubra-glacier-vault");
		

	    client.createVault(request);
		
	    OutputStream s = new GlacierMultipartBufferedOutputStream(client, "akubra-glacier-vault", URI.create("info:fedora/object:pid/dsID.0"));
		
		//byte[] b = new byte[1024*1024];
		//new Random().nextBytes(b);
	    
		
		s.write("1234567890".getBytes("ASCII"));
		s.close();
		
		InputStream s_in = new GlacierInputStream(client, "akubra-glacier-vault", URI.create("info:fedora/object:pid/dsID.0"));

		byte[] b1 = new byte[10];
		s_in.read(b1);
		s_in.close();
		
		assertArrayEquals("1234567890".getBytes("ASCII"), b1);
	}

}
