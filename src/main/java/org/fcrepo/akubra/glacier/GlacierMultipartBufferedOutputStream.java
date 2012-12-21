package org.fcrepo.akubra.glacier;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadResult;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadResult;
import com.amazonaws.services.glacier.model.UploadMultipartPartRequest;
import com.amazonaws.util.BinaryUtils;

public class GlacierMultipartBufferedOutputStream extends OutputStream {
    byte[] buffer;
    private int ioBufferSize = 1024*1024*2;
    private int bufferCurrentPos = 0;
    private URI blobId;
    private long currentObjectSize = 0;
	private AmazonGlacierClient glacier;
	private String upload_id;
	private String vault;
	private String archiveId = null;
    List<byte[]> binaryChecksums = new LinkedList<byte[]>();
	private GlacierBlobStoreConnection connection;


	public GlacierMultipartBufferedOutputStream(GlacierBlobStoreConnection connection, String vault, URI blobId) {
		this.connection = connection;
		this.glacier = connection.getGlacierClient();
		this.blobId = blobId;
		this.vault = vault;
		this.buffer = new byte[ioBufferSize];
		initiateMultipartUpload();
	}
	
	public String getArchiveId() {
		return this.archiveId;
	}
	
	private void initiateMultipartUpload() {
		InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest()
			.withArchiveDescription(blobId.toString())
			.withPartSize(Integer.toString(ioBufferSize))
			.withVaultName(vault);
		InitiateMultipartUploadResult result = glacier.initiateMultipartUpload(request);
		
		this.upload_id = result.getUploadId();
	}
	
	private void completeMultipartUpload() {
		CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest()
		     .withUploadId(upload_id)
		     .withArchiveSize(Long.toString(currentObjectSize))
		     .withVaultName(vault)
		     .withChecksum(getUploadChecksum());
		CompleteMultipartUploadResult response = glacier.completeMultipartUpload(request);
		this.archiveId = response.getArchiveId();
	}
    private String getUploadChecksum() {
    	return TreeHashGenerator.calculateTreeHash(binaryChecksums);
	}

	/**
     * Implement this far more efficiently than the ridiculous implementation in
     * the superclass.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        len = Math.min(b.length, len);
        off = Math.min(b.length - 1, off);
        for (int i = off; len > 0;) {
            int chunkLen = Math.min(len, buffer.length - bufferCurrentPos);
            System.arraycopy(b, i, buffer, bufferCurrentPos, chunkLen);
            bufferCurrentPos += chunkLen;
            if (bufferCurrentPos >= buffer.length - 1) {
                flush();
            }
            len -= chunkLen;
            i += chunkLen;
        }
    }

    @Override
    public void flush() {
        if (bufferCurrentPos > 0) {
        	String range = "bytes " + currentObjectSize + "-" + (currentObjectSize + bufferCurrentPos - 1) + "/*";
        	InputStream is = new ByteArrayInputStream(buffer);
        	String checksum = TreeHashGenerator.calculateTreeHash(is);
        	byte[] binaryChecksum = BinaryUtils.fromHex(checksum);
			binaryChecksums.add(binaryChecksum);
        	try {
				is.reset();
			} catch (IOException e) {
				is = new ByteArrayInputStream(buffer);
			}
        	
        	UploadMultipartPartRequest request = new UploadMultipartPartRequest().withRange(range)
        			.withVaultName(vault)
        			.withUploadId(upload_id)
        			.withBody(is)
        			.withChecksum(checksum);
        	
            glacier.uploadMultipartPart(request);
            
            currentObjectSize += bufferCurrentPos;
            bufferCurrentPos = 0;
        }
    }

    /**
     * Closes the stream, then notifies the CloseListener and call sync method
     * of BlobStoreConnection to write data.
     */
    @Override
    public void close() throws IOException {
    	flush();
    	completeMultipartUpload();
    }

    @Override
    public void write(int b) throws IOException {
        byte[] intValue = intToByteArray(b);
        write(intValue, 0, intValue.length);
    }

    protected byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }
}
