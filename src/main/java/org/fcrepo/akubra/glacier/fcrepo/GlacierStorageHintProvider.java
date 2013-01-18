package org.fcrepo.akubra.glacier.fcrepo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.fcrepo.akubra.glacier.GlacierBlobStore;
import org.fcrepo.server.storage.FedoraStorageHintProvider;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;

/**
 * GlacierHintsProvider
 * @author ba2213
 * The Glacier blob store will inspect a datastream version's altIDs
 * to determine whether it is to be stored on the file system or in
 * AWS-Glacier. If the altIDs contain a value of the pattern:
 * "info:fedora/${pid}/${dsid}?store=aws-glacier"
 * then storage in Glacier will be attempted.
 */
public class GlacierStorageHintProvider implements FedoraStorageHintProvider {
    /**
     * Since the about-to-be-stored DS is the latest one,
     * we can derive hints from the most recent/latest version.
     */
    public Map<String, String> getHintsForAboutToBeStoredDatastream(
            DigitalObject obj, String dsid) {
        Datastream ds = null;

        Iterator<Datastream> versions = obj.datastreams(dsid).iterator();
        String glacierFlag = "info:fedora/" + obj.getPid() + "/" + dsid + "?store=aws-glacier";
        long latest = -1;
        while (versions.hasNext()) {
            Datastream temp_ds = versions.next();
            if (temp_ds.DSCreateDT.getTime() > latest) {
                ds = temp_ds;
                latest = ds.DSCreateDT.getTime();
            }
        }
        // now get the altIDs of the latest version for hints
        for (String altId:ds.DatastreamAltIDs){
            if (glacierFlag.equals(altId.trim())) {
                Map<String, String> result = new HashMap<String,String>(1);
                result.put(GlacierBlobStore.STORAGE_HINT_KEY, GlacierBlobStore.STORAGE_HINT_VALUE_GLACIER);
                return result;
            }
        }
        return new HashMap<String,String>(0);
    }

    public Map<String, String> getHintsForAboutToBeStoredObject(
            DigitalObject arg0) {
        return new HashMap<String,String>(0);
    }

}
