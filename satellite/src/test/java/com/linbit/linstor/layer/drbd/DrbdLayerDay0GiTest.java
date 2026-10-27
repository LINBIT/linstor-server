package com.linbit.linstor.layer.drbd;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DrbdLayerDay0GiTest
{
    @Test
    public void initialCreationUsesDay0Gi()
    {
        // not initialized yet: day0 GI is used (and required, missing GI still fails the metadata creation)
        assertThat(DrbdLayer.useDay0GiAsLoser(false, true)).isTrue();
        assertThat(DrbdLayer.useDay0GiAsLoser(false, false)).isTrue();
    }

    @Test
    public void addedAfterInitializationWithGiUsesDay0Gi()
    {
        // unchanged behavior if the volume definition has a GI
        assertThat(DrbdLayer.useDay0GiAsLoser(true, true)).isTrue();
    }

    @Test
    public void addedAfterInitializationWithoutGiSyncsFromPeers()
    {
        assertThat(DrbdLayer.useDay0GiAsLoser(true, false)).isFalse();
    }
}
