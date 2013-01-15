package org.fcrepo.akubra.glacier;
import static org.junit.Assert.*;

import org.junit.Test;
import java.io.IOException;
import com.amazonaws.services.glacier.*;
import com.amazonaws.services.glacier.model.*;
import com.amazonaws.auth.PropertiesCredentials;

public class TestIcemeltGlacierMock {

	@Test
	public void testCreateVault() throws IOException {
		PropertiesCredentials credentials = new PropertiesCredentials(TestIcemeltGlacierMock.class.getResourceAsStream("AwsCredentials.properties"));
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint("http://localhost:3000/");
		
		CreateVaultRequest request = new CreateVaultRequest()
	    .withAccountId("-")
	    .withVaultName("akubra-glacier-vault");
	    CreateVaultResult result = client.createVault(request);
	}

}
