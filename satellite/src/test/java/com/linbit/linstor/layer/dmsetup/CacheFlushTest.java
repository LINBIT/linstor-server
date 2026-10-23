package com.linbit.linstor.layer.dmsetup;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.layer.dmsetup.cache.CacheLayer;
import com.linbit.linstor.layer.dmsetup.writecache.WritecacheLayer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Sub-root cache layers flush directly around the storage snapshot (round trip 2), not when the root layer is
 * suspended (round trip 1).
 */
@SuppressWarnings("unchecked")
public class CacheFlushTest
{
    private Resource rsc;
    private StateFlags<Resource.Flags> rscFlags;
    private Map<String, String> rscPropsMap;
    private AbsRscLayerObject<Resource> rscRoot;

    private ResourceDefinition rscDfn;
    private Snapshot snap;
    private StateFlags<Snapshot.Flags> snapFlags;
    private AbsRscLayerObject<Snapshot> snapRoot;
    private AbsRscLayerObject<Snapshot> snapStorage;

    private DeviceHandler deviceHandler;
    private ErrorReporter errorReporter;
    private ExtCmdFactory extCmdFactory;
    private ApiCallRcImpl apiCallRc;

    @Before
    public void setUp()
    {
        rsc = mock(Resource.class);
        rscFlags = mock(StateFlags.class);
        rscPropsMap = new HashMap<>();
        Props rscProps = mock(Props.class);
        when(rscProps.map()).thenReturn(rscPropsMap);
        when(rsc.getStateFlags()).thenReturn(rscFlags);
        when(rsc.getProps()).thenReturn(rscProps);

        rscRoot = layerData(DeviceLayerKind.DRBD, null, rsc);
        when(rscRoot.getShouldSuspendIo()).thenReturn(true);
        when(rsc.getLayerData()).thenReturn(rscRoot);

        NodeName nodeName = mock(NodeName.class);
        rscDfn = mock(ResourceDefinition.class);
        when(rscDfn.getResource(nodeName)).thenReturn(rsc);

        snap = mock(Snapshot.class);
        snapFlags = mock(StateFlags.class);
        when(snapFlags.isUnset(Snapshot.Flags.DELETE)).thenReturn(true);
        when(snap.getFlags()).thenReturn(snapFlags);
        when(snap.getTakeSnapshot()).thenReturn(true);
        when(snap.getNodeName()).thenReturn(nodeName);
        when(snap.getResourceDefinition()).thenReturn(rscDfn);
        snapRoot = layerData(DeviceLayerKind.DRBD, null, snap);

        deviceHandler = mock(DeviceHandler.class);
        errorReporter = mock(ErrorReporter.class);
        extCmdFactory = mock(ExtCmdFactory.class);
        apiCallRc = new ApiCallRcImpl();
    }

    private static <RSC extends AbsResource<RSC>> AbsRscLayerObject<RSC> layerData(
        DeviceLayerKind kind,
        AbsRscLayerObject<RSC> parent,
        RSC absRsc
    )
    {
        return initLayerData(mock(AbsRscLayerObject.class), kind, parent, absRsc);
    }

    private static <RSC extends AbsResource<RSC>, T extends AbsRscLayerObject<RSC>>
        T initLayerData(T data, DeviceLayerKind kind, AbsRscLayerObject<RSC> parent, RSC absRsc)
    {
        when(data.getLayerKind()).thenReturn(kind);
        when(data.getResourceNameSuffix()).thenReturn(RscLayerSuffixes.SUFFIX_DATA);
        when(data.getParent()).thenReturn(parent);
        when(data.getAbsResource()).thenReturn(absRsc);
        when(data.getChildren()).thenReturn(Collections.emptySet());
        when(data.exists()).thenReturn(true);
        if (parent != null)
        {
            when(parent.getChildren()).thenReturn(Set.<AbsRscLayerObject<RSC>>of(data));
        }
        return data;
    }

    /** DRBD -> CACHE -> STORAGE for resource and snapshot; returns the snapshot's cache layer data */
    private AbsRscLayerObject<Snapshot> cacheStack(AbsRscLayerObject<Resource> rscCache)
    {
        initLayerData(rscCache, DeviceLayerKind.CACHE, rscRoot, rsc);
        AbsRscLayerObject<Snapshot> snapCache = layerData(DeviceLayerKind.CACHE, snapRoot, snap);
        snapStorage = layerData(DeviceLayerKind.STORAGE, snapCache, snap);
        return snapCache;
    }

    // --- CacheFlushUtils: snapshot ---

    @Test
    public void snapshotFlushNeededInRoundTrip2()
    {
        AbsRscLayerObject<Resource> rscCache = mock(AbsRscLayerObject.class);
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(rscCache);

        assertThat(CacheFlushUtils.getRscDataToFlushForSnapshot(snapCache)).isSameAs(rscCache);
    }

    @Test
    public void snapshotNoFlushWhenNotTakingSnapshot()
    {
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(mock(AbsRscLayerObject.class));
        when(snap.getTakeSnapshot()).thenReturn(false);

        assertThat(CacheFlushUtils.getRscDataToFlushForSnapshot(snapCache)).isNull();
    }

    @Test
    public void snapshotNoFlushWhenDeleting()
    {
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(mock(AbsRscLayerObject.class));
        when(snapFlags.isUnset(Snapshot.Flags.DELETE)).thenReturn(false);

        assertThat(CacheFlushUtils.getRscDataToFlushForSnapshot(snapCache)).isNull();
    }

    @Test
    public void snapshotNoFlushAfterResume()
    {
        // round trip 3: the root layer is resumed, takeSnapshot is still set until the cleanup round trip
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(mock(AbsRscLayerObject.class));
        when(rscRoot.getShouldSuspendIo()).thenReturn(false);

        assertThat(CacheFlushUtils.getRscDataToFlushForSnapshot(snapCache)).isNull();
    }

    @Test
    public void snapshotNoFlushForRootCache()
    {
        // a root cache layer is suspended including its flush in round trip 1
        AbsRscLayerObject<Snapshot> snapCache = layerData(DeviceLayerKind.CACHE, null, snap);

        assertThat(CacheFlushUtils.getRscDataToFlushForSnapshot(snapCache)).isNull();
    }

    @Test
    public void snapshotNoFlushForInactiveResource()
    {
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(mock(AbsRscLayerObject.class));
        // a single inactive flag is enough
        when(
            rscFlags.isSomeSet(
                Resource.Flags.INACTIVE,
                Resource.Flags.INACTIVE_PERMANENTLY,
                Resource.Flags.INACTIVATING
            )
        ).thenReturn(true);

        assertThat(CacheFlushUtils.getRscDataToFlushForSnapshot(snapCache)).isNull();
    }

    @Test
    public void snapshotNoFlushWithoutLocalResource()
    {
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(mock(AbsRscLayerObject.class));
        when(rscDfn.getResource(any())).thenReturn(null);

        assertThat(CacheFlushUtils.getRscDataToFlushForSnapshot(snapCache)).isNull();
    }

    @Test
    public void snapshotNoFlushWhenCacheDeviceMissing()
    {
        AbsRscLayerObject<Resource> rscCache = mock(AbsRscLayerObject.class);
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(rscCache);
        when(rscCache.exists()).thenReturn(false);

        assertThat(CacheFlushUtils.getRscDataToFlushForSnapshot(snapCache)).isNull();
    }

    // --- CacheFlushUtils: clone ---

    @Test
    public void cloneFlushNeededInRoundTrip2()
    {
        AbsRscLayerObject<Resource> rscCache = layerData(DeviceLayerKind.CACHE, rscRoot, rsc);
        rscPropsMap.put(InternalApiConsts.CLONE_PROP_PREFIX + "target", "target");

        assertThat(CacheFlushUtils.isCloneSnapshotPending(rscCache)).isTrue();
    }

    @Test
    public void cloneNoFlushWithoutCloneProp()
    {
        AbsRscLayerObject<Resource> rscCache = layerData(DeviceLayerKind.CACHE, rscRoot, rsc);

        assertThat(CacheFlushUtils.isCloneSnapshotPending(rscCache)).isFalse();
    }

    @Test
    public void cloneNoFlushWhenNotSuspended()
    {
        AbsRscLayerObject<Resource> rscCache = layerData(DeviceLayerKind.CACHE, rscRoot, rsc);
        rscPropsMap.put(InternalApiConsts.CLONE_PROP_PREFIX + "target", "target");
        when(rscRoot.getShouldSuspendIo()).thenReturn(false);

        assertThat(CacheFlushUtils.isCloneSnapshotPending(rscCache)).isFalse();
    }

    @Test
    public void cloneNoFlushForRootLayer()
    {
        rscPropsMap.put(InternalApiConsts.CLONE_PROP_PREFIX + "target", "target");

        assertThat(CacheFlushUtils.isCloneSnapshotPending(rscRoot)).isFalse();
    }

    // --- CacheLayer (dm-cache) ---

    private CacheLayer cacheLayerSpy() throws Exception
    {
        CacheLayer layer = spy(
            new CacheLayer(errorReporter, extCmdFactory, () -> deviceHandler, mock(StltConfigAccessor.class))
        );
        doNothing().when(layer).suspendIo(any(), anyBoolean());
        doNothing().when(layer).resumeIo(any(), anyBoolean());
        return layer;
    }

    @Test
    public void cacheSnapshotFlushesSuspendsAndResumesAroundStorageSnapshot() throws Exception
    {
        AbsRscLayerObject<Resource> rscCache = mock(CacheRscData.class);
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(rscCache);
        CacheLayer layer = cacheLayerSpy();

        boolean processedChildren = layer.processSnapshot(snapCache, apiCallRc);

        assertThat(processedChildren).isTrue();
        InOrder order = inOrder(layer, deviceHandler);
        order.verify(layer).suspendIo(rscCache, false);
        order.verify(deviceHandler).processSnapshot(snapStorage, apiCallRc);
        order.verify(layer).resumeIo(rscCache, false);
    }

    @Test
    public void cacheSnapshotWithoutFlushLeavesChildrenToDeviceHandler() throws Exception
    {
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(mock(CacheRscData.class));
        when(rscRoot.getShouldSuspendIo()).thenReturn(false);
        CacheLayer layer = cacheLayerSpy();

        boolean processedChildren = layer.processSnapshot(snapCache, apiCallRc);

        assertThat(processedChildren).isFalse();
        verify(layer, never()).suspendIo(any(), anyBoolean());
        verify(deviceHandler, never()).processSnapshot(any(), any());
    }

    @Test
    public void cacheSnapshotResumesWhenSnapshotFails() throws Exception
    {
        AbsRscLayerObject<Resource> rscCache = mock(CacheRscData.class);
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(rscCache);
        CacheLayer layer = cacheLayerSpy();
        StorageException snapExc = new StorageException("snapshot failed");
        doThrow(snapExc).when(deviceHandler).processSnapshot(snapStorage, apiCallRc);

        assertThatThrownBy(() -> layer.processSnapshot(snapCache, apiCallRc)).isSameAs(snapExc);
        verify(layer).resumeIo(rscCache, false);
    }

    @Test
    public void cacheSnapshotResumesWhenFlushFails() throws Exception
    {
        AbsRscLayerObject<Resource> rscCache = mock(CacheRscData.class);
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(rscCache);
        CacheLayer layer = cacheLayerSpy();
        doThrow(new ExtCmdFailedException(new String[] {"dmsetup"}, new IOException("flush timed out")))
            .when(layer).suspendIo(rscCache, false);

        assertThatThrownBy(() -> layer.processSnapshot(snapCache, apiCallRc))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("Failed to flush cache");
        verify(deviceHandler, never()).processSnapshot(any(), any());
        // the cleaner policy has to be replaced by the default policy again
        verify(layer).resumeIo(rscCache, false);
    }

    @Test
    public void cacheSnapshotKeepsOriginalExceptionWhenResumeFailsToo() throws Exception
    {
        AbsRscLayerObject<Resource> rscCache = mock(CacheRscData.class);
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(rscCache);
        CacheLayer layer = cacheLayerSpy();
        StorageException snapExc = new StorageException("snapshot failed");
        doThrow(snapExc).when(deviceHandler).processSnapshot(snapStorage, apiCallRc);
        doThrow(new StorageException("resume failed")).when(layer).resumeIo(rscCache, false);

        assertThatThrownBy(() -> layer.processSnapshot(snapCache, apiCallRc)).isSameAs(snapExc);
        verify(errorReporter).reportError(any(StorageException.class));
    }

    @Test
    public void cacheSnapshotThrowsResumeFailureAfterSuccessfulSnapshot() throws Exception
    {
        AbsRscLayerObject<Resource> rscCache = mock(CacheRscData.class);
        AbsRscLayerObject<Snapshot> snapCache = cacheStack(rscCache);
        CacheLayer layer = cacheLayerSpy();
        doThrow(new StorageException("resume failed")).when(layer).resumeIo(rscCache, false);

        assertThatThrownBy(() -> layer.processSnapshot(snapCache, apiCallRc))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("Failed to resume cache");
    }

    @Test
    public void cacheCloneFlushesAroundDataChild() throws Exception
    {
        CacheRscData<Resource> rscCache = mock(CacheRscData.class);
        initLayerData(rscCache, DeviceLayerKind.CACHE, rscRoot, rsc);
        AbsRscLayerObject<Resource> dataChild = layerData(DeviceLayerKind.STORAGE, rscCache, rsc);
        when(rscCache.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA)).thenReturn(dataChild);
        rscPropsMap.put(InternalApiConsts.CLONE_PROP_PREFIX + "target", "target");
        CacheLayer layer = cacheLayerSpy();

        layer.processResource(rscCache, apiCallRc);

        InOrder order = inOrder(layer, deviceHandler);
        order.verify(layer).suspendIo(rscCache, false);
        order.verify(deviceHandler).processResource(dataChild, apiCallRc);
        order.verify(layer).resumeIo(rscCache, false);
    }

    @Test
    public void cacheWithoutCloneDoesNotFlush() throws Exception
    {
        CacheRscData<Resource> rscCache = mock(CacheRscData.class);
        initLayerData(rscCache, DeviceLayerKind.CACHE, rscRoot, rsc);
        AbsRscLayerObject<Resource> dataChild = layerData(DeviceLayerKind.STORAGE, rscCache, rsc);
        when(rscCache.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA)).thenReturn(dataChild);
        CacheLayer layer = cacheLayerSpy();

        layer.processResource(rscCache, apiCallRc);

        verify(deviceHandler).processResource(dataChild, apiCallRc);
        verify(layer, never()).suspendIo(any(), anyBoolean());
        verify(layer, never()).resumeIo(any(), anyBoolean());
    }

    // --- WritecacheLayer ---

    private WritecacheLayer writecacheLayer()
    {
        return new WritecacheLayer(errorReporter, extCmdFactory, () -> deviceHandler, mock(StltConfigAccessor.class));
    }

    @Test
    public void writecacheSnapshotFlushesAndLeavesChildrenToDeviceHandler() throws Exception
    {
        AbsRscLayerObject<Resource> rscWc = mock(WritecacheRscData.class);
        AbsRscLayerObject<Snapshot> snapWc = cacheStack(rscWc);
        when(rscWc.getLayerKind()).thenReturn(DeviceLayerKind.WRITECACHE);
        when(snapWc.getLayerKind()).thenReturn(DeviceLayerKind.WRITECACHE);

        try (MockedStatic<DmSetupUtils> dmSetup = mockStatic(DmSetupUtils.class))
        {
            boolean processedChildren = writecacheLayer().processSnapshot(snapWc, apiCallRc);

            assertThat(processedChildren).isFalse();
            dmSetup.verify(() -> DmSetupUtils.flush(extCmdFactory, rscWc));
            // no suspend: the writecache stays usable for DRBD's own writes
            dmSetup.verify(() -> DmSetupUtils.suspendIo(any(), any(), any(AbsRscLayerObject.class), eq(true), any()),
                never());
        }
    }

    @Test
    public void writecacheSnapshotWithoutFlush() throws Exception
    {
        AbsRscLayerObject<Resource> rscWc = mock(WritecacheRscData.class);
        AbsRscLayerObject<Snapshot> snapWc = cacheStack(rscWc);
        when(rscWc.getLayerKind()).thenReturn(DeviceLayerKind.WRITECACHE);
        when(snapWc.getLayerKind()).thenReturn(DeviceLayerKind.WRITECACHE);
        when(snap.getTakeSnapshot()).thenReturn(false);

        try (MockedStatic<DmSetupUtils> dmSetup = mockStatic(DmSetupUtils.class))
        {
            assertThat(writecacheLayer().processSnapshot(snapWc, apiCallRc)).isFalse();
            dmSetup.verify(() -> DmSetupUtils.flush(any(), any(AbsRscLayerObject.class)), never());
        }
    }

    @Test
    public void writecacheCloneFlushesBeforeDataChild() throws Exception
    {
        WritecacheRscData<Resource> rscWc = mock(WritecacheRscData.class);
        initLayerData(rscWc, DeviceLayerKind.WRITECACHE, rscRoot, rsc);
        AbsRscLayerObject<Resource> dataChild = layerData(DeviceLayerKind.STORAGE, rscWc, rsc);
        when(rscWc.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA)).thenReturn(dataChild);
        rscPropsMap.put(InternalApiConsts.CLONE_PROP_PREFIX + "target", "target");

        try (MockedStatic<DmSetupUtils> dmSetup = mockStatic(DmSetupUtils.class))
        {
            dmSetup.when(() -> DmSetupUtils.flush(extCmdFactory, rscWc)).then(
                invocation ->
                {
                    // the data child (and with it the clone snapshot) must not have been processed yet
                    verify(deviceHandler, never()).processResource(any(), any());
                    return null;
                }
            );

            writecacheLayer().processResource(rscWc, apiCallRc);

            dmSetup.verify(() -> DmSetupUtils.flush(extCmdFactory, rscWc));
            verify(deviceHandler).processResource(dataChild, apiCallRc);
        }
    }
}
