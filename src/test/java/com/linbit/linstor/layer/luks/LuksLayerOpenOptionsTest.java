package com.linbit.linstor.layer.luks;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.test.factories.ResourceTestFactory;
import com.linbit.linstor.test.factories.StorPoolTestFactory;
import com.linbit.linstor.test.factories.VolumeTestFactory;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LuksLayerOpenOptionsTest extends GenericDbBase
{
    private static final String PROP_LUKS_ALLOW_DISCARDS = ApiConsts.NAMESPC_LUKS + "/" +
        ApiConsts.KEY_LUKS_ALLOW_DISCARDS;
    private static final String PROP_LUKS_OPEN_OPTIONS = ApiConsts.NAMESPC_STORAGE_DRIVER + "/" +
        ApiConsts.KEY_STOR_DRIVER_LUKS_OPEN_OPTIONS;

    private static final String NODE_NAME = "node";
    private static final String RSC_NAME = "rsc";
    private static final String SP_NAME = "mysp";
    private static final int VLM_NR = 0;

    @Inject ResourceTestFactory rscFactory;
    @Inject VolumeTestFactory vlmFactory;
    @Inject StorPoolTestFactory spFactory;
    @Inject EncryptionHelper encrHelper;

    private LuksLayer luksLayer;

    @Before
    public void setup() throws Exception
    {
        setUpAndEnterScope();

        byte[] testMasterKey = encrHelper.generateSecret();
        encrHelper.setPassphraseImpl("testPassphrase".getBytes(), testMasterKey, SYS_CTX);
        @Nullable ReadOnlyProps encryptedNamespace = encrHelper.getEncryptedNamespace(SYS_CTX);
        if (encryptedNamespace == null)
        {
            throw new ImplementationError(
                "encryption namespace must not be null after encrHelper.setPassphraseImpl was called"
            );
        }
        encrHelper.setCryptKey(testMasterKey, encryptedNamespace, false);

        StltConfigAccessor mockedStltConfigAccessor = Mockito.mock(StltConfigAccessor.class);
        Mockito.when(mockedStltConfigAccessor.getReadonlyProps())
            .thenReturn(ReadOnlyPropsImpl.emptyRoProps());

        luksLayer = new LuksLayer(
            SYS_CTX,
            Mockito.mock(CryptSetupCommands.class),
            Mockito.mock(ExtCmdFactory.class),
            (Provider<DeviceHandler>) () -> Mockito.mock(DeviceHandler.class),
            Mockito.mock(ErrorReporter.class),
            Mockito.mock(StltSecurityObjects.class),
            Mockito.mock(DecryptionHelper.class),
            mockedStltConfigAccessor
        );
    }

    @Test
    public void propertyUnsetAddsNoFlag() throws Exception
    {
        LuksVlmData<Resource> vlmData = buildLuksVlm();

        assertFalse(luksLayer.isAllowDiscardsEnabled(vlmData));
        List<String> opts = luksLayer.getOpenOptions(vlmData);
        assertFalse("--allow-discards must not be present when property is unset", opts.contains("--allow-discards"));
    }

    @Test
    public void propertyTrueOnResourceGroupAddsFlag() throws Exception
    {
        LuksVlmData<Resource> vlmData = buildLuksVlm();
        Volume vlm = (Volume) vlmData.getVolume();
        ResourceGroup rscGrp = vlm.getResourceDefinition().getResourceGroup();
        rscGrp.getProps(SYS_CTX).setProp(PROP_LUKS_ALLOW_DISCARDS, ApiConsts.VAL_TRUE);

        assertTrue(luksLayer.isAllowDiscardsEnabled(vlmData));
        List<String> opts = luksLayer.getOpenOptions(vlmData);
        assertTrue(opts.contains("--allow-discards"));
    }

    @Test
    public void lowerScopeFalseOverridesControllerTrue() throws Exception
    {
        LuksVlmData<Resource> vlmData = buildLuksVlm();
        Volume vlm = (Volume) vlmData.getVolume();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();

        rscGrp.getProps(SYS_CTX).setProp(PROP_LUKS_ALLOW_DISCARDS, ApiConsts.VAL_TRUE);
        rscDfn.getProps(SYS_CTX).setProp(PROP_LUKS_ALLOW_DISCARDS, ApiConsts.VAL_FALSE);

        assertFalse(luksLayer.isAllowDiscardsEnabled(vlmData));
        List<String> opts = luksLayer.getOpenOptions(vlmData);
        assertFalse("resource-definition false must override resource-group true", opts.contains("--allow-discards"));
    }

    @Test
    public void flagDeduplicatedWhenAlsoInLuksOpenOptions() throws Exception
    {
        LuksVlmData<Resource> vlmData = buildLuksVlm();
        Volume vlm = (Volume) vlmData.getVolume();
        ResourceGroup rscGrp = vlm.getResourceDefinition().getResourceGroup();

        rscGrp.getProps(SYS_CTX).setProp(PROP_LUKS_ALLOW_DISCARDS, ApiConsts.VAL_TRUE);
        rscGrp.getProps(SYS_CTX).setProp(PROP_LUKS_OPEN_OPTIONS, "--allow-discards");

        List<String> opts = luksLayer.getOpenOptions(vlmData);
        long count = opts.stream().filter("--allow-discards"::equals).count();
        assertEquals("--allow-discards must appear exactly once", 1, count);
    }

    @Test
    public void propertyTrueCoexistsWithOtherLuksOpenOptions() throws Exception
    {
        LuksVlmData<Resource> vlmData = buildLuksVlm();
        Volume vlm = (Volume) vlmData.getVolume();
        ResourceGroup rscGrp = vlm.getResourceDefinition().getResourceGroup();

        rscGrp.getProps(SYS_CTX).setProp(PROP_LUKS_ALLOW_DISCARDS, ApiConsts.VAL_TRUE);
        rscGrp.getProps(SYS_CTX).setProp(PROP_LUKS_OPEN_OPTIONS, "--header-backup-file /tmp/x");

        List<String> opts = luksLayer.getOpenOptions(vlmData);
        assertTrue(opts.contains("--allow-discards"));
        assertTrue(opts.contains("--header-backup-file"));
        assertTrue(opts.contains("/tmp/x"));
    }

    @SuppressWarnings("unchecked")
    private LuksVlmData<Resource> buildLuksVlm() throws Exception
    {
        Node node = nodeTestFactory.builder(NODE_NAME).build();

        // LuksLayerSizeCalculator requires cryptsetup ext-tools info to be available via the peer
        Peer mockedPeer = Mockito.mock(Peer.class);
        ExtToolsManager mockedExtToolsMgr = Mockito.mock(ExtToolsManager.class);
        Mockito.when(mockedPeer.getExtToolsManager()).thenReturn(mockedExtToolsMgr);
        Mockito.when(mockedExtToolsMgr.getExtToolInfo(ExtTools.CRYPT_SETUP))
            .thenReturn(
                new ExtToolsInfo(ExtTools.CRYPT_SETUP, true, new ExtToolsInfo.Version(2, 4, 3), null)
            );
        node.setPeer(SYS_CTX, mockedPeer);

        Resource rsc = rscFactory.builder(NODE_NAME, RSC_NAME)
            .setLayerStack(List.of(DeviceLayerKind.LUKS, DeviceLayerKind.STORAGE))
            .build();
        StorPool storPool = spFactory.builder(NODE_NAME, SP_NAME)
            .setDriverKind(DeviceProviderKind.LVM)
            .build();
        volumeDefinitionTestFactory.builder(RSC_NAME)
            .setVlmNr(VLM_NR)
            .setSize(10L * 1024L)
            .build();
        LayerPayload payload = new LayerPayload();
        payload.putStorageVlmPayload("", VLM_NR, storPool);
        vlmFactory.builder(NODE_NAME, RSC_NAME, VLM_NR)
            .setLayerPayload(payload)
            .build();

        Set<AbsRscLayerObject<Resource>> luksData = LayerRscUtils.getRscDataByLayer(
            rsc.getLayerData(SYS_CTX),
            DeviceLayerKind.LUKS
        );
        LuksRscData<Resource> luksRscData = (LuksRscData<Resource>) luksData.iterator().next();
        return luksRscData.getVlmProviderObject(new VolumeNumber(VLM_NR));
    }
}
