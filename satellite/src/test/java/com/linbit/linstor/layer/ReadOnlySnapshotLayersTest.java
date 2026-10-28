package com.linbit.linstor.layer;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.layer.dmsetup.cache.CacheLayer;
import com.linbit.linstor.layer.dmsetup.writecache.WritecacheLayer;
import com.linbit.linstor.layer.luks.CryptSetupCommands;
import com.linbit.linstor.layer.luks.LuksLayer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The layer part of {@link DeviceHandler#openSnapshotReadOnly}: LUKS maps the snapshot below read-only, cache layers
 * pass through to their data child.
 */
@SuppressWarnings("unchecked")
public class ReadOnlySnapshotLayersTest
{
    private static final String RSC_NAME = "rsc";
    private static final String LUKS_ID = RSC_NAME + "_00000";
    private static final String SNAP_NAME = "snap1";
    /** the snapshot name keeps the mappings of different snapshots of the same resource apart */
    private static final String SNAP_MAPPING = LUKS_ID + "_" + SNAP_NAME + "_ro";
    private static final String STORAGE_SNAP_DEV = "/dev/vg/rsc_00000_snap";
    private static final byte[] MASTER_KEY = {1, 2, 3};
    private static final byte[] ENCRYPTED_KEY = {4, 5, 6};
    private static final byte[] SNAPSHOT_KEY = {7, 8, 9};
    private static final byte[] RESOURCE_KEY = {10, 11, 12};

    private DeviceHandler deviceHandler;
    private CryptSetupCommands cryptSetup;
    private StltSecurityObjects secObjs;
    private DecryptionHelper decryptionHelper;
    private LuksLayer luksLayer;
    private VlmProviderObject<?> storageChild;
    private ReadOnlySnapshotHandle childHandle;

    @Before
    public void setUp() throws Exception
    {
        deviceHandler = mock(DeviceHandler.class);
        cryptSetup = mock(CryptSetupCommands.class);
        secObjs = mock(StltSecurityObjects.class);
        decryptionHelper = mock(DecryptionHelper.class);
        luksLayer = spy(
            new LuksLayer(
                cryptSetup,
                mock(ExtCmdFactory.class),
                () -> deviceHandler,
                mock(ErrorReporter.class),
                secObjs,
                decryptionHelper,
                mock(StltConfigAccessor.class)
            )
        );
        storageChild = mock(VlmProviderObject.class);
        when(storageChild.getCloneDevicePath()).thenReturn(STORAGE_SNAP_DEV);
        when(cryptSetup.getLuksVolumePath(any())).thenAnswer(inv -> "/dev/mapper/Linstor-Crypt-" + inv.getArgument(0));
        // real default methods (closeAfterFailure), the abstract close() does nothing
        childHandle = mock(ReadOnlySnapshotHandle.class, Answers.CALLS_REAL_METHODS);
        when(deviceHandler.openSnapshotReadOnly(any())).thenReturn(childHandle);
    }

    /** The storage child as seen below snapshot volume data */
    private VlmProviderObject<Snapshot> snapStorageChild()
    {
        return (VlmProviderObject<Snapshot>) storageChild;
    }

    /** The storage child as seen below resource volume data */
    private VlmProviderObject<Resource> rscStorageChild()
    {
        return (VlmProviderObject<Resource>) storageChild;
    }

    private LuksVlmData<Snapshot> luksSnapshotVlm() throws Exception
    {
        return luksSnapshotVlm(SNAP_NAME);
    }

    private LuksVlmData<Snapshot> luksSnapshotVlm(String snapName) throws Exception
    {
        Snapshot snap = mock(Snapshot.class);
        when(snap.getSnapshotName()).thenReturn(new SnapshotName(snapName));
        LuksRscData<Snapshot> rscData = mock(LuksRscData.class);
        when(rscData.getAbsResource()).thenReturn(snap);
        when(rscData.getSuffixedResourceName()).thenReturn(RSC_NAME);
        LuksVlmData<Snapshot> vlm = mock(LuksVlmData.class);
        doReturn(rscData).when(vlm).getRscLayerObject();
        when(vlm.getVlmNr()).thenReturn(new VolumeNumber(0));
        when(vlm.getEncryptedKey()).thenReturn(ENCRYPTED_KEY);
        doReturn(storageChild).when(vlm).getSingleChild();
        return vlm;
    }

    private LuksVlmData<Resource> luksResourceVlm() throws Exception
    {
        LuksRscData<Resource> rscData = mock(LuksRscData.class);
        when(rscData.getSuffixedResourceName()).thenReturn(RSC_NAME);
        LuksVlmData<Resource> vlm = mock(LuksVlmData.class);
        doReturn(rscData).when(vlm).getRscLayerObject();
        when(vlm.getVlmNr()).thenReturn(new VolumeNumber(0));
        when(vlm.getIdentifier()).thenReturn(LUKS_ID);
        when(vlm.getDecryptedPassword()).thenReturn(RESOURCE_KEY);
        doReturn(storageChild).when(vlm).getSingleChild();
        return vlm;
    }

    @Test
    public void luksSnapshotIsMappedReadOnlyWithItsOwnKey() throws Exception
    {
        // the snapshot's key is not decrypted when the master key arrives, and may differ from the resource's key
        LuksVlmData<Snapshot> vlm = luksSnapshotVlm();
        when(secObjs.getCryptKey()).thenReturn(MASTER_KEY);
        when(decryptionHelper.decrypt(MASTER_KEY, ENCRYPTED_KEY)).thenReturn(SNAPSHOT_KEY);
        luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(vlm, "_ro"));

        // the layer below gets the same opener with its own volume data
        verify(deviceHandler).openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapStorageChild(), "_ro"));
        // no open options: a snapshot has no volume properties of its own
        verify(cryptSetup).openLuksDevice(STORAGE_SNAP_DEV, SNAP_MAPPING, SNAPSHOT_KEY, true, List.of());
        verify(vlm).setDecryptedPassword(SNAPSHOT_KEY);
        verify(vlm).setCloneDevicePath("/dev/mapper/Linstor-Crypt-" + SNAP_MAPPING);
        // the name of the resource's mapping is not stored as the snapshot's own mapping
        verify(vlm, never()).setIdentifier(any());
    }

    @Test
    public void luksSnapshotWithoutMasterKeyFails() throws Exception
    {
        LuksVlmData<Snapshot> vlm = luksSnapshotVlm();

        assertThatThrownBy(() -> luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(vlm, "_ro")))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("master key");
        verify(cryptSetup, never()).openLuksDevice(any(), any(), any(), anyBoolean(), any());
        // the snapshot below was already opened
        verify(childHandle).close();
    }

    @Test
    public void luksFailedMappingClosesSnapshotBelow() throws Exception
    {
        LuksVlmData<Resource> vlm = luksResourceVlm();
        doReturn(List.of()).when(luksLayer).getOpenOptions(vlm);
        StorageException openFailure = new StorageException("cryptsetup open failed");
        doThrow(openFailure).when(cryptSetup).openLuksDevice(any(), any(), any(), anyBoolean(), any());
        StorageException closeFailure = new StorageException("close failed");
        doThrow(closeFailure).when(childHandle).close();

        assertThatThrownBy(() -> luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forClone(vlm, "target")))
            .isSameAs(openFailure);
        verify(childHandle).close();
        assertThat(openFailure.getSuppressed()).containsExactly(closeFailure);
    }

    @Test
    public void luksFailedCloneTargetClosesTargetBelow() throws Exception
    {
        LuksVlmData<Resource> vlm = luksResourceVlm();
        doReturn(List.of()).when(luksLayer).getCreateOptions(vlm);
        StorageException createFailure = new StorageException("luksFormat failed");
        doThrow(createFailure).when(cryptSetup).createLuksDevice(any(), any(), any(), any());

        assertThatThrownBy(() -> luksLayer.openCloneTarget(vlm)).isSameAs(createFailure);
        verify(deviceHandler).openCloneTarget(rscStorageChild());
        verify(deviceHandler).closeCloneTarget(rscStorageChild());
    }

    @Test
    public void luksCloneSourceKeepsCloneMappingNameAndResourceKey() throws Exception
    {
        LuksVlmData<Resource> vlm = luksResourceVlm();
        doReturn(List.of("--perf-no_read_workqueue")).when(luksLayer).getOpenOptions(vlm);
        luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forClone(vlm, "target"));

        String mapping = LUKS_ID + InternalApiConsts.CLONE_FOR_PREFIX + "target";
        verify(deviceHandler).openSnapshotReadOnly(ReadOnlyOpener.forClone(rscStorageChild(), "target"));
        verify(cryptSetup).openLuksDevice(
            STORAGE_SNAP_DEV,
            mapping,
            RESOURCE_KEY,
            true,
            List.of("--perf-no_read_workqueue")
        );
        verify(decryptionHelper, never()).decrypt(any(), any());
    }

    @Test
    public void luksSnapshotsOfSameResourceGetDifferentMappings() throws Exception
    {
        when(secObjs.getCryptKey()).thenReturn(MASTER_KEY);
        when(decryptionHelper.decrypt(MASTER_KEY, ENCRYPTED_KEY)).thenReturn(SNAPSHOT_KEY);
        luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(luksSnapshotVlm("snap1"), "_ro"));
        luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(luksSnapshotVlm("snap2"), "_ro"));

        verify(cryptSetup).openLuksDevice(STORAGE_SNAP_DEV, LUKS_ID + "_snap1_ro", SNAPSHOT_KEY, true, List.of());
        verify(cryptSetup).openLuksDevice(STORAGE_SNAP_DEV, LUKS_ID + "_snap2_ro", SNAPSHOT_KEY, true, List.of());
    }

    @Test
    public void luksAlreadyOpenMappingIsReused() throws Exception
    {
        LuksVlmData<Snapshot> vlm = luksSnapshotVlm();
        when(cryptSetup.isOpen(SNAP_MAPPING)).thenReturn(true);
        luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(vlm, "_ro"));

        // the snapshot below is opened anyway, so that closing the handle is symmetric
        verify(deviceHandler).openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapStorageChild(), "_ro"));
        verify(cryptSetup, never()).openLuksDevice(any(), any(), any(), anyBoolean(), any());
        verify(vlm).setCloneDevicePath("/dev/mapper/Linstor-Crypt-" + SNAP_MAPPING);
    }

    @Test
    public void luksCloseClosesMappingAndSnapshotBelow() throws Exception
    {
        LuksVlmData<Snapshot> vlm = luksSnapshotVlm();
        when(secObjs.getCryptKey()).thenReturn(MASTER_KEY);
        when(decryptionHelper.decrypt(MASTER_KEY, ENCRYPTED_KEY)).thenReturn(SNAPSHOT_KEY);
        // not open yet when opening, open when closing
        when(cryptSetup.isOpen(SNAP_MAPPING)).thenReturn(false, true);

        ReadOnlySnapshotHandle handle = luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(vlm, "_ro"));
        verify(childHandle, never()).close();
        handle.close();

        verify(cryptSetup).closeLuksDevice(SNAP_MAPPING);
        verify(childHandle).close();
        verify(vlm).setCloneDevicePath(null);
    }

    @Test
    public void luksFailedCloseStillClosesSnapshotBelow() throws Exception
    {
        LuksVlmData<Snapshot> vlm = luksSnapshotVlm();
        when(secObjs.getCryptKey()).thenReturn(MASTER_KEY);
        when(decryptionHelper.decrypt(MASTER_KEY, ENCRYPTED_KEY)).thenReturn(SNAPSHOT_KEY);
        when(cryptSetup.isOpen(SNAP_MAPPING)).thenReturn(false, true);
        StorageException closeFailure = new StorageException("cryptsetup close failed");
        doThrow(closeFailure).when(cryptSetup).closeLuksDevice(SNAP_MAPPING);

        ReadOnlySnapshotHandle handle = luksLayer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(vlm, "_ro"));

        assertThatThrownBy(handle::close).isSameAs(closeFailure);
        verify(childHandle).close();
        verify(vlm).setCloneDevicePath(null);
    }

    // --- cache layers pass through to the data child ---

    private VlmProviderObject<Snapshot> cacheSnapshotVlm(VlmProviderObject<Snapshot> dataChildVlm) throws Exception
    {
        VolumeNumber vlmNr = new VolumeNumber(0);
        AbsRscLayerObject<Snapshot> dataChildRsc = mock(AbsRscLayerObject.class);
        doReturn(dataChildVlm).when(dataChildRsc).getVlmProviderObject(vlmNr);
        AbsRscLayerObject<Snapshot> cacheRsc = mock(AbsRscLayerObject.class);
        when(cacheRsc.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA)).thenReturn(dataChildRsc);
        VlmProviderObject<Snapshot> cacheVlm = mock(VlmProviderObject.class);
        when(cacheVlm.getRscLayerObject()).thenReturn(cacheRsc);
        when(cacheVlm.getVlmNr()).thenReturn(vlmNr);
        return cacheVlm;
    }

    @Test
    public void cacheAndWritecachePassThroughToDataChild() throws Exception
    {
        for (DeviceLayer layer : List.of(
            new CacheLayer(mock(ErrorReporter.class), mock(ExtCmdFactory.class), () -> deviceHandler, null),
            new WritecacheLayer(mock(ErrorReporter.class), mock(ExtCmdFactory.class), () -> deviceHandler, null)
        ))
        {
            VlmProviderObject<Snapshot> dataChild = mock(VlmProviderObject.class);
            when(dataChild.getCloneDevicePath()).thenReturn(STORAGE_SNAP_DEV);
            VlmProviderObject<Snapshot> cacheVlm = cacheSnapshotVlm(dataChild);
            ReadOnlySnapshotHandle dataChildHandle = mock(ReadOnlySnapshotHandle.class);
            when(deviceHandler.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(dataChild, "_ro")))
                .thenReturn(dataChildHandle);

            ReadOnlySnapshotHandle handle = layer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(cacheVlm, "_ro"));
            verify(cacheVlm).setCloneDevicePath(STORAGE_SNAP_DEV);

            handle.close();
            verify(dataChildHandle).close();
            verify(cacheVlm).setCloneDevicePath(null);
        }
    }

    @Test
    public void unsupportedLayerRejectsReadOnlyOpen()
    {
        DeviceLayer drbdLikeLayer = mock(DeviceLayer.class, Answers.CALLS_REAL_METHODS);
        when(drbdLikeLayer.getKind()).thenReturn(DeviceLayerKind.DRBD);

        assertThatThrownBy(
            () -> drbdLikeLayer.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(mock(VlmProviderObject.class), "_ro"))
        ).isInstanceOf(StorageException.class).hasMessageContaining("DRBD");
    }
}
