package org.fcrepo.akubra.glacier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;

import org.fcrepo.akubra.glacier.fcrepo.GlacierStorageHintProvider;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestGlacierHintsProvider {
    private static final String objPid = "test:1";
    private static final String dsid_glacier = "DS1";
    private static final String dsid_synch = "DS2";

    private static final String objUri = "info:fedora/" + objPid;
    private static final String dsUri = objUri + "/" + dsid_glacier;
    private static final String[] altIDs = new String[]{dsUri + "?store=aws-glacier"};

    @Mock
    private DigitalObject object;

    @Mock
    private Datastream ds_glacier;

    @Mock
    private Datastream ds_synch;

    @Test
    public void testHints() {
        ds_glacier.DatastreamAltIDs = altIDs;
        when(object.getPid()).thenReturn(objPid);
        when(object.datastreams(dsid_glacier)).thenReturn(Arrays.asList(new Datastream[]{ds_glacier}));
        GlacierStorageHintProvider test = new GlacierStorageHintProvider();
        Map<String,String> actual = test.getHintsForAboutToBeStoredDatastream(object, dsid_glacier);
        assertTrue(actual.containsKey(GlacierBlobStore.STORAGE_HINT_KEY));
        assertEquals(1, actual.size());
        assertEquals(GlacierBlobStore.STORAGE_HINT_VALUE_GLACIER, actual.get(GlacierBlobStore.STORAGE_HINT_KEY));
    }

    @Test
    public void testSynchHints() {
        when(object.getPid()).thenReturn(objPid);
        when(object.datastreams(dsid_synch)).thenReturn(Arrays.asList(new Datastream[]{ds_synch}));
        ds_synch.DatastreamAltIDs = new String[0];
        GlacierStorageHintProvider test = new GlacierStorageHintProvider();
        Map<String,String> actual = test.getHintsForAboutToBeStoredDatastream(object, dsid_synch);
        assertTrue(actual.isEmpty());
    }

    @Test
    public void testObjectHints(){
        when(object.getPid()).thenReturn(objPid);
        GlacierStorageHintProvider test = new GlacierStorageHintProvider();
        Map<String,String> actual = test.getHintsForAboutToBeStoredObject(object);
        assertTrue(actual.isEmpty());
    }

}
