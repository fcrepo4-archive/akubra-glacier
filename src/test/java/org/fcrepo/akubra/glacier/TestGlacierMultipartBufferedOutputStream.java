package org.fcrepo.akubra.glacier;


import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Random;

import org.fcrepo.akubra.glacier.GlacierMultipartBufferedOutputStream;
import org.junit.Test;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.CreateVaultRequest;
import com.amazonaws.services.glacier.model.CreateVaultResult;

public class TestGlacierMultipartBufferedOutputStream {

	@Test
	public void test() throws IOException {
		PropertiesCredentials credentials = new PropertiesCredentials(TestGlacierMultipartBufferedOutputStream.class.getResourceAsStream("AwsCredentials.properties"));
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint("http://localhost:3000/");
	
		CreateVaultRequest request = new CreateVaultRequest()
			.withAccountId("-")
			.withVaultName("akubra-glacier-vault");
		

	    client.createVault(request);
	    
		OutputStream s = new GlacierMultipartBufferedOutputStream(client, "akubra-glacier-vault", URI.create("info:fedora/object:pid/dsID.0"));
		
		byte[] b = new byte[1024*1024*4];
		new Random().nextBytes(b);
		
		s.write(b);
		s.close();
	}

}
