package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.utils.Base64;

import jakarta.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LayerLuksDbDriverTest extends GenericDbBase
{
    private static final String LAYER_RESOURCE_ID = "LAYER_RESOURCE_ID";
    private static final String ENCRYPTED_PASSWORD = "ENCRYPTED_PASSWORD";

    private static final String SELECT_ALL_LUKS_VOLUMES =
        " SELECT " + LAYER_RESOURCE_ID + ", " + VLM_NR + ", " + ENCRYPTED_PASSWORD +
        " FROM " + TBL_LAYER_LUKS_VOLUMES;

    private static final String NODE_NAME_STR = "node";
    private static final String RSC_NAME_STR = "rsc";
    private static final String SP_NAME_STR = "mySp";
    private static final int VLM_NR_INT = 0;
    private static final long VLM_SIZE = 10 * 1024L;

    @Inject
    private EncryptionHelper encrHelper;

    private Resource rsc;
    private LuksRscData<Resource> luksRscData;
    private LuksVlmData<Resource> luksVlmData;

    @Before
    @SuppressWarnings({"unchecked", "checkstyle:magicnumber"})
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();

        byte[] testMasterKey = encrHelper.generateSecret();
        encrHelper.setPassphraseImpl("testPassphrase".getBytes(StandardCharsets.UTF_8), testMasterKey);
        ReadOnlyProps encryptedNamespace = encrHelper.getEncryptedNamespace();
        if (encryptedNamespace == null)
        {
            throw new ImplementationError(
                "encryption namespace must not be null after encrHelper.setPassphraseImpl was called"
            );
        }
        encrHelper.setCryptKey(testMasterKey, encryptedNamespace, false);

        Node node = nodeTestFactory.builder(NODE_NAME_STR).build();
        Peer mockedPeer = Mockito.mock(Peer.class);
        ExtToolsManager mockedExtToolsMgr = Mockito.mock(ExtToolsManager.class);
        Mockito.when(mockedPeer.getExtToolsManager()).thenReturn(mockedExtToolsMgr);
        Mockito.when(mockedExtToolsMgr.getExtToolInfo(ExtTools.CRYPT_SETUP))
            .thenReturn(
                new ExtToolsInfo(ExtTools.CRYPT_SETUP, true, new ExtToolsInfo.Version(2, 4, 3), null)
            );
        node.setPeer(mockedPeer);

        rsc = resourceTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR)
            .setLayerStack(Arrays.asList(DeviceLayerKind.LUKS, DeviceLayerKind.STORAGE))
            .build();
        StorPool storPool = storPoolTestFactory.builder(NODE_NAME_STR, SP_NAME_STR)
            .setDriverKind(DeviceProviderKind.LVM)
            .build();
        volumeTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR, VLM_NR_INT)
            .setSize(VLM_SIZE)
            .setStorPoolData(storPool)
            .build();

        luksRscData = (LuksRscData<Resource>) rsc.getLayerData();
        luksVlmData = luksRscData.getVlmProviderObject(new VolumeNumber(VLM_NR_INT));

        commit();
    }

    @Test
    public void testPersistedLuksVlm() throws Exception
    {
        assertNotNull(luksVlmData.getEncryptedKey());

        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_LUKS_VOLUMES);
            ResultSet resultSet = stmt.executeQuery())
        {
            assertTrue(resultSet.next());
            assertEquals(luksRscData.getRscLayerId(), resultSet.getInt(LAYER_RESOURCE_ID));
            assertEquals(VLM_NR_INT, resultSet.getInt(VLM_NR));
            assertEquals(Base64.encode(luksVlmData.getEncryptedKey()), resultSet.getString(ENCRYPTED_PASSWORD));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testLuksLayerResourceIds() throws Exception
    {
        AbsRscLayerObject<Resource> storRscData = luksRscData.getChildren().iterator().next();
        assertEquals(DeviceLayerKind.LUKS, luksRscData.getLayerKind());
        assertEquals(DeviceLayerKind.STORAGE, storRscData.getLayerKind());

        try (PreparedStatement stmt = getConnection().prepareStatement(
                "SELECT COUNT(*) FROM " + TBL_LAYER_RESOURCE_IDS);
            ResultSet resultSet = stmt.executeQuery())
        {
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
        }
    }

    @Test
    public void testUpdateEncryptedPassword() throws Exception
    {
        byte[] newKey = "anotherEncryptedKey".getBytes(StandardCharsets.UTF_8);

        luksVlmData.setEncryptedKey(newKey);
        commit();

        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_LUKS_VOLUMES);
            ResultSet resultSet = stmt.executeQuery())
        {
            assertTrue(resultSet.next());
            assertEquals(Base64.encode(newKey), resultSet.getString(ENCRYPTED_PASSWORD));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testDelete() throws Exception
    {
        rsc.delete();
        commit();

        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_LUKS_VOLUMES);
            ResultSet resultSet = stmt.executeQuery())
        {
            assertFalse(resultSet.next());
        }
    }
}
