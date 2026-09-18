package com.linbit.linstor.layer.drbd.helper;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.layer.drbd.resfiles.DrbdResourceFileUtils;
import com.linbit.linstor.layer.drbd.utils.DrbdAdm;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DrbdadmAdjustTest
{
    private static final String RSC_NAME = "rsc";
    private static final String REJECTED_CONTENT = """
        resource "rsc"
        {
            net
            {
                cram-hmac-alg     no-such-algorithm;
            }
        }
        """;

    private ErrorReporter errorReporter;
    private DrbdAdm drbdAdm;
    private DrbdResourceFileUtils resFileUtils;
    private DrbdRscData<Resource> rscData;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception
    {
        errorReporter = Mockito.mock(ErrorReporter.class);
        drbdAdm = Mockito.mock(DrbdAdm.class);
        resFileUtils = Mockito.mock(DrbdResourceFileUtils.class);
        rscData = Mockito.mock(DrbdRscData.class);

        Mockito.when(rscData.getSuffixedResourceName()).thenReturn(RSC_NAME);
        Mockito.when(resFileUtils.getResFilePath(rscData)).thenReturn(Paths.get("/var/lib/linstor.d/rsc.res"));
        Mockito.when(resFileUtils.readChangedResFileContent(rscData)).thenReturn(REJECTED_CONTENT);

        String[] cmd = {"drbdadm", "-vvv", "adjust", RSC_NAME};
        OutputData out = new OutputData(
            cmd,
            new byte[0],
            "rsc.res:5: parse error".getBytes(StandardCharsets.UTF_8),
            1
        );
        Mockito.doThrow(new ExtCmdFailedException(cmd, out))
            .when(drbdAdm).adjust(rscData, false, false, false);
    }

    private DrbdadmAdjust adjuster()
    {
        return new DrbdadmAdjust(errorReporter, drbdAdm, resFileUtils, rscData);
    }

    private ExtCmdFailedException runExpectingFailure(DrbdadmAdjust adjust) throws StorageException
    {
        ExtCmdFailedException ret = null;
        try
        {
            adjust.adjust();
            fail("adjust must rethrow the drbdadm failure");
        }
        catch (ExtCmdFailedException exc)
        {
            ret = exc;
        }
        assertNotNull(ret);
        return ret;
    }

    @Test
    public void changedResFileIsAttachedBeforeTheBackupIsRestored() throws Exception
    {
        ExtCmdFailedException exc = runExpectingFailure(adjuster().withRestoreResFileOnFailure(true));

        Throwable[] suppressed = exc.getSuppressed();
        assertEquals(1, suppressed.length);
        assertTrue(suppressed[0] instanceof StorageException);
        StorageException attached = (StorageException) suppressed[0];
        assertTrue(attached.getMessage().contains(RSC_NAME));
        String details = attached.getDetailsText();
        assertNotNull(details);
        assertTrue(details.contains("/var/lib/linstor.d/rsc.res"));
        assertTrue(details.contains(REJECTED_CONTENT));

        // the content must be read while the rejected file is still on disk
        InOrder inOrder = Mockito.inOrder(resFileUtils);
        inOrder.verify(resFileUtils).readChangedResFileContent(rscData);
        inOrder.verify(resFileUtils).restoreBackupResFile(rscData);
    }

    @Test
    public void nothingIsAttachedOrRestoredWithoutRestoreOnFailure() throws Exception
    {
        ExtCmdFailedException exc = runExpectingFailure(adjuster().withRestoreResFileOnFailure(false));

        assertEquals(0, exc.getSuppressed().length);
        Mockito.verify(resFileUtils, Mockito.never()).readChangedResFileContent(rscData);
        Mockito.verify(resFileUtils, Mockito.never()).restoreBackupResFile(rscData);
    }

    @Test
    public void unchangedOrUnreadableResFileStillRestoresTheBackup() throws Exception
    {
        Mockito.when(resFileUtils.readChangedResFileContent(rscData)).thenReturn(null);

        ExtCmdFailedException exc = runExpectingFailure(adjuster().withRestoreResFileOnFailure(true));

        assertEquals(0, exc.getSuppressed().length);
        Mockito.verify(resFileUtils).restoreBackupResFile(rscData);
    }
}
