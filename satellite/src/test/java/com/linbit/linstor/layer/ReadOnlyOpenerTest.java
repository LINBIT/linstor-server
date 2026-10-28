package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class ReadOnlyOpenerTest
{
    @Test
    public void snapshotMappingSuffixContainsSnapshotName() throws Exception
    {
        assertThat(ReadOnlyOpener.forSnapshot(snapshotVlmData("snap1"), "_ro").mappingSuffix())
            .isEqualTo("_snap1_ro");
    }

    @Test
    public void cloneMappingSuffixContainsCloneTarget()
    {
        assertThat(ReadOnlyOpener.forClone(mock(VlmProviderObject.class), "target").mappingSuffix())
            .isEqualTo(InternalApiConsts.CLONE_FOR_PREFIX + "target");
    }

    @Test
    public void withVlmDataKeepsEverythingButTheVolumeData()
    {
        VlmProviderObject<Snapshot> snapVlm = mock(VlmProviderObject.class);
        VlmProviderObject<Snapshot> snapChildVlm = mock(VlmProviderObject.class);
        assertThat(ReadOnlyOpener.forSnapshot(snapVlm, "_ro").withVlmData(snapChildVlm))
            .isEqualTo(ReadOnlyOpener.forSnapshot(snapChildVlm, "_ro"));

        VlmProviderObject<Resource> rscVlm = mock(VlmProviderObject.class);
        VlmProviderObject<Resource> rscChildVlm = mock(VlmProviderObject.class);
        assertThat(ReadOnlyOpener.forClone(rscVlm, "target").withVlmData(rscChildVlm))
            .isEqualTo(ReadOnlyOpener.forClone(rscChildVlm, "target"));
    }

    @Test
    public void snapshotOpenerSuffixMustNotBeEmptyOrTooLong()
    {
        VlmProviderObject<Snapshot> vlmData = mock(VlmProviderObject.class);
        int maxLen = ReadOnlyOpener.ForSnapshot.MAX_OPENER_SUFFIX_LENGTH;
        assertThatThrownBy(() -> ReadOnlyOpener.forSnapshot(vlmData, "")).isInstanceOf(ImplementationError.class);
        assertThatThrownBy(() -> ReadOnlyOpener.forSnapshot(vlmData, "x".repeat(maxLen + 1)))
            .isInstanceOf(ImplementationError.class);
        assertThatCode(() -> ReadOnlyOpener.forSnapshot(vlmData, "x".repeat(maxLen))).doesNotThrowAnyException();
    }

    private static VlmProviderObject<Snapshot> snapshotVlmData(String snapName) throws Exception
    {
        Snapshot snap = mock(Snapshot.class);
        when(snap.getSnapshotName()).thenReturn(new SnapshotName(snapName));
        AbsRscLayerObject<Snapshot> rscData = mock(AbsRscLayerObject.class);
        doReturn(snap).when(rscData).getAbsResource();
        VlmProviderObject<Snapshot> vlmData = mock(VlmProviderObject.class);
        doReturn(rscData).when(vlmData).getRscLayerObject();
        return vlmData;
    }
}
