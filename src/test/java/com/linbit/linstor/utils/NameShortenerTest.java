package com.linbit.linstor.utils;

import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.SatellitePropDriver;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

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
        NameShortener shorter = new NameShortener("", key, 5, TestAccessContextProvider.SYS_CTX, "_", null);

        addAndAssert(shorter, "rsc", "", key, "rsc"); // < limit
        addAndAssert(shorter, "rscTooLong", "", key, "rsc_1");
        addAndAssert(shorter, "rscTooLong42", "", key, "rsc_2");
        addAndAssert(shorter, "rscTooLong43", "", key, "rsc_3");
        addAndAssert(shorter, "rscTooLong44", "", key, "rsc_4");
        addAndAssert(shorter, "rscTooLong45", "", key, "rsc_5");
        addAndAssert(shorter, "rscTooLong46", "", key, "rsc_6");
        addAndAssert(shorter, "rscTooLong47", "", key, "rsc_7");
        addAndAssert(shorter, "rscTooLong48", "", key, "rsc_8");
        addAndAssert(shorter, "rscTooLong49", "", key, "rsc_9");
        addAndAssert(shorter, "rscTooLong50", "", key, "rs_10");
        addAndAssert(shorter, "rscTooLong70", "", key, "rs_11");

        addAndAssert(shorter, "otherRsc", "", key, "oth_1");

        addAndAssert(shorter, "oth_1", "", key, "oth_2");

        addAndAssert(shorter, "ot_10", "", key, "ot_10");

        addAndAssert(shorter, "otherRsc02", "", key, "oth_3");
        ResourceDefinition otherRsc03 = addAndAssert(shorter, "otherRsc03", "", key, "oth_4");
        addAndAssert(shorter, "otherRsc04", "", key, "oth_5");
        addAndAssert(shorter, "otherRsc05", "", key, "oth_6");
        addAndAssert(shorter, "otherRsc06", "", key, "oth_7");
        addAndAssert(shorter, "otherRsc32", "", key, "oth_8");
        addAndAssert(shorter, "otherRsc42", "", key, "oth_9");
        addAndAssert(shorter, "otherRsc90", "", key, "ot_11");
        addAndAssert(shorter, "ot_11", "", key, "ot_12");

        addAndAssert(shorter, "ot", "42", key, "ot42");
        addAndAssert(shorter, "oth", "9001", key, "ot_13");

        addAndAssert(shorter, "ab", "9001", key, "ab9_1");

        shorter.remove(otherRsc03, "");
        addAndAssert(shorter, "otherRsc03", "", key, "oth_4");
        addAndAssert(shorter, "otherRsc03", "", key, "oth_4");
    }

    @Test
    public void sharedBaseTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 5, TestAccessContextProvider.SYS_CTX, "_", null);

        addAndAssert(shorter, "abbxx1", "", key, "abb_1");
        addAndAssert(shorter, "abbxx2", "", key, "abb_2");
        addAndAssert(shorter, "abbxx3", "", key, "abb_3");

        addAndAssert(shorter, "abcxx1", "", key, "abc_1");
        addAndAssert(shorter, "abcxx2", "", key, "abc_2");
        addAndAssert(shorter, "abcxx3", "", key, "abc_3");
        addAndAssert(shorter, "abcxx4", "", key, "abc_4");
        addAndAssert(shorter, "abcxx5", "", key, "abc_5");
        addAndAssert(shorter, "abcxx6", "", key, "abc_6");
        addAndAssert(shorter, "abcxx7", "", key, "abc_7");
        addAndAssert(shorter, "abcxx8", "", key, "abc_8");
        addAndAssert(shorter, "abcxx9", "", key, "abc_9");
        addAndAssert(shorter, "abcx10", "", key, "ab_10");
        addAndAssert(shorter, "abcx11", "", key, "ab_11");

        addAndAssert(shorter, "abbxx4", "", key, "abb_4");
        addAndAssert(shorter, "abbxx5", "", key, "abb_5");
        addAndAssert(shorter, "abbxx6", "", key, "abb_6");
        addAndAssert(shorter, "abbxx7", "", key, "abb_7");
        addAndAssert(shorter, "abbxx8", "", key, "abb_8");
        addAndAssert(shorter, "abbxx9", "", key, "abb_9");
        addAndAssert(shorter, "abbx10", "", key, "ab_12");
        addAndAssert(shorter, "abbx11", "", key, "ab_13");
    }

    @Test
    public void invalidCharsTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 5, TestAccessContextProvider.SYS_CTX, "_", "a-zA-Z");

        addAndAssert(shorter, "abc123", "", key, "abc");
        addAndAssert(shorter, "abc124", "", key, "abc_1");
        addAndAssert(shorter, "abc125", "", key, "abc_2");
    }

    @Test
    public void notTooLongTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 5, TestAccessContextProvider.SYS_CTX, "_", "a-zA-Z");
        addAndAssert(shorter, "ab", "", key, "ab");
        addAndAssert(shorter, "abc", "", key, "abc");
    }

    @Test
    public void namespaceTest() throws Throwable
    {
        String key = "key";
        NameShortener shorter = new NameShortener("", key, 5, TestAccessContextProvider.SYS_CTX, "_", "a-zA-Z");
        addAndAssert(shorter, "ab", 0, "", "ns1", key, "ab");
        addAndAssert(shorter, "ab", 0, "", "ns2", key, "ab_1");
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
        ResourceDefinition rscDfn = rscDfnMap.get(rscName);
        if (rscDfn == null)
        {
            rscDfn = mock(rscName);
            rscDfnMap.put(rscName, rscDfn);
        }

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
        String expectedShortenedRscName
    )
        throws Throwable
    {
        ResourceDefinition rscDfn = rscDfnMap.get(rscName);
        if (rscDfn == null)
        {
            rscDfn = mock(rscName);
            rscDfnMap.put(rscName, rscDfn);
        }
        VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(TestAccessContextProvider.SYS_CTX, new VolumeNumber(0));
        if (vlmDfn == null) {
            vlmDfn = mock(rscDfn, vlmNr);
        }

        String shortenedName = nameShortener.shorten(vlmDfn, propKeyPrefix, rscSuffix);

        assertEquals(expectedShortenedRscName, shortenedName);
        assertEquals(
            vlmDfn.getProps(TestAccessContextProvider.SYS_CTX).getProp(propKeyPrefix + propKey),
            shortenedName
        );

        return vlmDfn;
    }

    private ResourceDefinition mock(String rscNameRef) throws Throwable
    {
        ResourceDefinition mockedRscDfn = Mockito.mock(ResourceDefinition.class);
        ResourceName resName = new ResourceName(rscNameRef);
        Mockito.when(mockedRscDfn.getName()).thenReturn(resName);
        String propsPath = PropsContainer.buildPath(resName);
        Mockito.when(mockedRscDfn.getProps(Mockito.any())).thenReturn(propsContainerFactory.create(propsPath));
        return mockedRscDfn;
    }

    private VolumeDefinition mock(ResourceDefinition rscDfnRef, int vlmNrInt) throws Throwable
    {
        VolumeDefinition mockedVlmDfn = Mockito.mock(VolumeDefinition.class);
        Mockito.when(mockedVlmDfn.getResourceDefinition()).thenReturn(rscDfnRef);
        VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);
        Mockito.when(mockedVlmDfn.getVolumeNumber()).thenReturn(vlmNr);
        String propsPath = PropsContainer.buildPath(rscDfnRef.getName(), vlmNr);
        Mockito.when(mockedVlmDfn.getProps(Mockito.any())).thenReturn(propsContainerFactory.create(propsPath));
        Mockito.when(rscDfnRef.getVolumeDfnCount(Mockito.any())).thenReturn(1);
        Mockito.when(rscDfnRef.getVolumeDfn(Mockito.any(), Mockito.eq(vlmNr))).thenReturn(mockedVlmDfn);
        return mockedVlmDfn;
    }
}
