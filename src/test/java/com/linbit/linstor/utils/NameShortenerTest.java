package com.linbit.linstor.utils;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.SatellitePropDriver;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NameShortenerTest
{
    private final PropsContainerFactory propsContainerFactory;
    private final Map<String, ResourceDefinition> rscDfnMap;

    public NameShortenerTest()
    {
        final SatelliteTransactionMgr satelliteTransactionMgr = new SatelliteTransactionMgr();
        propsContainerFactory = new PropsContainerFactory(
            new SatellitePropDriver(),
            () -> satelliteTransactionMgr
        );
        rscDfnMap = new TreeMap<>();
    }

    @Test
    public void simpleTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 10, TestAccessContextProvider.SYS_CTX, "_", null);

        addAndAssert(shorter, "rsc", "", key, "rsc"); // < limit
        addAndAssert(shorter, "rscTooLong00", "", key, "rsc_1");
        addAndAssert(shorter, "rscTooLong42", "", key, "rsc_2");
        addAndAssert(shorter, "rscTooLong43", "", key, "rsc_3");
        addAndAssert(shorter, "rscTooLong44", "", key, "rsc_4");
        addAndAssert(shorter, "rscTooLong45", "", key, "rsc_5");
        addAndAssert(shorter, "rscTooLong46", "", key, "rsc_6");
        addAndAssert(shorter, "rscTooLong47", "", key, "rsc_7");
        addAndAssert(shorter, "rscTooLong48", "", key, "rsc_8");
        addAndAssert(shorter, "rscTooLong49", "", key, "rsc_9");
        addAndAssert(shorter, "rscTooLong50", "", key, "rsc_10");
        addAndAssert(shorter, "rscTooLong70", "", key, "rsc_11");

        addAndAssert(shorter, "otherRsc", "", key, "otherRsc");

        addAndAssert(shorter, "oth_10", "", key, "oth_10");

        addAndAssert(shorter, "oth_1", "", key, "oth_1");
        addAndAssert(shorter, "oth_2", "", key, "oth_2");
        addAndAssert(shorter, "oth_3", "", key, "oth_3");

        ResourceDefinition oth4 = addAndAssert(shorter, "otherWayTooLongResourceName01", "", key, "oth_4");
        addAndAssert(shorter, "otherWayTooLongResourceName02", "", key, "oth_5");
        addAndAssert(shorter, "otherWayTooLongResourceName03", "", key, "oth_6");
        addAndAssert(shorter, "otherWayTooLongResourceName04", "", key, "oth_7");
        addAndAssert(shorter, "otherWayTooLongResourceName05", "", key, "oth_8");
        addAndAssert(shorter, "otherWayTooLongResourceName06", "", key, "oth_9");
        addAndAssert(shorter, "otherWayTooLongResourceName07", "", key, "oth_11");
        addAndAssert(shorter, "otherWayTooLongResourceName08", "", key, "oth_12");
        addAndAssert(shorter, "otherWayTooLongResourceName09", "", key, "oth_13");

        addAndAssert(shorter, "ot", "42", key, "ot42");
        addAndAssert(shorter, "oth", "9001", key, "oth9001");

        addAndAssert(shorter, "abcdefg", "9001", key, "abc_1");

        shorter.remove(oth4, "");
        addAndAssert(shorter, "otherWayTooLongResourceName10", "", key, "oth_4");
        addAndAssert(shorter, "otherWayTooLongResourceName11", "", key, "oth_14");
    }

    @Test
    public void sharedBaseTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 10, TestAccessContextProvider.SYS_CTX, "_", null);

        addAndAssert(shorter, "aaatest000", "", key, "aaatest000");
        addAndAssert(shorter, "aaaatest001", "", key, "aaa_1");
        addAndAssert(shorter, "aaaatest002", "", key, "aaa_2");
        addAndAssert(shorter, "aaaatest003", "", key, "aaa_3");
        addAndAssert(shorter, "aaaatest004", "", key, "aaa_4");
        addAndAssert(shorter, "aaaatest005", "", key, "aaa_5");
        addAndAssert(shorter, "aaaatest006", "", key, "aaa_6");
        addAndAssert(shorter, "aaaatest007", "", key, "aaa_7");
        addAndAssert(shorter, "aaaatest008", "", key, "aaa_8");
        addAndAssert(shorter, "aaaatest009", "", key, "aaa_9");
        addAndAssert(shorter, "aaaatest010", "", key, "aaa_10");

        addAndAssert(shorter, "aabatest001", "", key, "aab_1");
        addAndAssert(shorter, "aabatest002", "", key, "aab_2");

        addAndAssert(shorter, "aaabdiff001", "", key, "aaa_11");
    }

    @Test
    public void invalidCharsTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 10, TestAccessContextProvider.SYS_CTX, "_", "a-zA-Z");

        addAndAssert(shorter, "abc123", "", key, "abc");
        addAndAssert(shorter, "abc124", "", key, "abc_1");
        addAndAssert(shorter, "abc125", "", key, "abc_2");
    }

    @Test
    public void notTooLongTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 10, TestAccessContextProvider.SYS_CTX, "_", "a-zA-Z");
        addAndAssert(shorter, "ab", "", key, "ab");
        addAndAssert(shorter, "abc", "", key, "abc");
        addAndAssert(shorter, "abcdef", "", key, "abcdef");
        addAndAssert(shorter, "abcdefghij", "", key, "abcdefghij");
    }

    @Test
    public void namespaceTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 10, TestAccessContextProvider.SYS_CTX, "_", "a-zA-Z");
        addAndAssert(shorter, "ab", 0, "", "ns1", key, "ab", false);
        addAndAssert(shorter, "ab", 0, "", "ns2", key, "ab_1", false);
    }

    @Test
    public void overrideVlmIdTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 32, TestAccessContextProvider.SYS_CTX, "_", "a-zA-Z");

        VolumeDefinition vlmDfn = getVlmDfn("rsc", 0);
        String overrideVlmId = "test";
        vlmDfn.getProps(TestAccessContextProvider.SYS_CTX)
            .setProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID, overrideVlmId);
        String keyPrefix = "";
        String shorten = shorter.shorten(vlmDfn, keyPrefix, "", false);

        assertEquals(overrideVlmId, shorten);
        assertEquals(overrideVlmId, vlmDfn.getProps(TestAccessContextProvider.SYS_CTX).getProp(keyPrefix + key));

        VolumeDefinition vlmDfn2 = getVlmDfn("rsc2", 0);
        vlmDfn2.getProps(TestAccessContextProvider.SYS_CTX)
            .setProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID, overrideVlmId);
        try
        {
            shorter.shorten(vlmDfn2, keyPrefix, "", false);
            fail("exception expected");
        }
        catch (LinStorException expected)
        {
            // expected
        }
    }

    private ResourceDefinition addAndAssert(
        NameShortener nameShortener,
        String rscName,
        String rscSuffix,
        String propKey,
        String expectedShortenedRscName
    )
        throws Throwable
    {
        ResourceDefinition rscDfn = getRscDfn(rscName);

        String shortenedName = nameShortener.shorten(rscDfn, rscSuffix);

        assertEquals(expectedShortenedRscName, shortenedName);
        assertEquals(
            rscDfn.getProps(TestAccessContextProvider.SYS_CTX).getProp(propKey),
            shortenedName
        );

        return rscDfn;
    }

    private VolumeDefinition addAndAssert(
        NameShortener nameShortener,
        String rscName,
        int vlmNr,
        String rscSuffix,
        String propKeyPrefix,
        String propKey,
        String expectedShortenedRscName,
        boolean appendVlmNr
    )
        throws Throwable
    {
        ResourceDefinition rscDfn = getRscDfn(rscName);
        VolumeDefinition vlmDfn = getVlmDfn(rscDfn, vlmNr);

        String shortenedName = nameShortener.shorten(vlmDfn, propKeyPrefix, rscSuffix, appendVlmNr);

        assertEquals(expectedShortenedRscName, shortenedName);
        assertEquals(
            vlmDfn.getProps(TestAccessContextProvider.SYS_CTX).getProp(propKeyPrefix + propKey),
            shortenedName
        );

        return vlmDfn;
    }

    private ResourceDefinition getRscDfn(String rscName) throws Throwable
    {
        ResourceDefinition rscDfn = rscDfnMap.get(rscName);
        if (rscDfn == null)
        {
            rscDfn = mock(rscName);
            rscDfnMap.put(rscName, rscDfn);
        }
        return rscDfn;
    }

    private VolumeDefinition getVlmDfn(String rscNameStr, int vlmNr)
        throws AccessDeniedException, ValueOutOfRangeException, Throwable
    {
        return getVlmDfn(getRscDfn(rscNameStr), vlmNr);
    }

    private VolumeDefinition getVlmDfn(ResourceDefinition rscDfn, int vlmNr)
        throws AccessDeniedException, ValueOutOfRangeException, Throwable
    {
        VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(TestAccessContextProvider.SYS_CTX, new VolumeNumber(0));
        if (vlmDfn == null)
        {
            vlmDfn = mock(rscDfn, vlmNr);
        }
        return vlmDfn;
    }

    private ResourceDefinition mock(String rscNameRef) throws Throwable
    {
        ResourceDefinition mockedRscDfn = Mockito.mock(ResourceDefinition.class);
        ResourceName resName = new ResourceName(rscNameRef);
        Mockito.when(mockedRscDfn.getName()).thenReturn(resName);
        String propsPath = PropsContainer.buildPath(resName);
        Mockito.when(mockedRscDfn.getProps(Mockito.any()))
            .thenReturn(propsContainerFactory.create(propsPath, null, LinStorObject.RSC_DFN));
        return mockedRscDfn;
    }

    private VolumeDefinition mock(ResourceDefinition rscDfnRef, int vlmNrInt) throws Throwable
    {
        VolumeDefinition mockedVlmDfn = Mockito.mock(VolumeDefinition.class);
        Mockito.when(mockedVlmDfn.getResourceDefinition()).thenReturn(rscDfnRef);
        VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);
        Mockito.when(mockedVlmDfn.getVolumeNumber()).thenReturn(vlmNr);
        String propsPath = PropsContainer.buildPath(rscDfnRef.getName(), vlmNr);
        Mockito.when(mockedVlmDfn.getProps(Mockito.any()))
            .thenReturn(propsContainerFactory.create(propsPath, null, LinStorObject.VLM_DFN));
        Mockito.when(rscDfnRef.getVolumeDfnCount(Mockito.any())).thenReturn(1);
        Mockito.when(rscDfnRef.getVolumeDfn(Mockito.any(), Mockito.eq(vlmNr))).thenReturn(mockedVlmDfn);
        return mockedVlmDfn;
    }
}
