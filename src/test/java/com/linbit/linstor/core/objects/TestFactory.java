package com.linbit.linstor.core.objects;

import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestFactory
{
    /**
     * Creates a new {@link NetInterface} without persisting it to the database
     */
    public static NetInterface createNetInterface(
        UUID niUuidRef,
        NetInterfaceName niNameRef,
        Node nodeRef,
        LsIpAddress niAddrRef,
        TcpPortNumber niStltConnPortRef,
        EncryptionType niStltConnEncrTypeRef,
        NetInterfaceGenericDbDriver dbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        return new NetInterface(
            niUuidRef,
            niNameRef,
            nodeRef,
            niAddrRef,
            niStltConnPortRef,
            niStltConnEncrTypeRef,
            dbDriverRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
    }

    /**
     * Creates a new {@link NodeConnection} without persisting it to the database
     * @throws DatabaseException
     */
    public static NodeConnection createNodeConnection(
        UUID uuidRef,
        Node nodeSrcRef,
        Node nodeDstRef,
        NodeConnectionGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        return new NodeConnection(
            uuidRef,
            nodeSrcRef,
            nodeDstRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
    }

    /**
     * Creates a new {@link Node} without persisting it to the database
     * @throws DatabaseException
     */
    public static Node createNode(
        UUID uuidRef,
        ObjectProtection objProtRef,
        NodeName nodeNameRef,
        Node.Type initialTypeRef,
        long initialFlagsRef,
        NodeGenericDbDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        return new Node(
            uuidRef,
            objProtRef,
            nodeNameRef,
            initialTypeRef,
            initialFlagsRef,
            dbDriverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
    }

    /**
     * Creates a new {@link ResourceConnection} without persisting it to the database
     * @throws DatabaseException
     */
    public static ResourceConnection createResourceConnection(
        UUID uuidRef,
        Resource resSrcRef,
        Resource resDstRef,
        TcpPortNumber portRef,
        DynamicNumberPool tcpPortPoolRef,
        ResourceConnectionGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        long initFlags
    )
        throws DatabaseException
    {
        return new ResourceConnection(
            uuidRef,
            resSrcRef,
            resDstRef,
            portRef,
            tcpPortPoolRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef,
            initFlags
        );
    }

    /**
     * Creates a new {@link Resource} without persisting it to the database
     * @throws DatabaseException
     */
    public static Resource createResource(
        UUID resUuidRef,
        ObjectProtection objProtRef,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        long initFlagsRef,
        ResourceGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<Resource.ResourceKey, ResourceConnection> rscConnMapRef,
        Map<VolumeNumber, Volume> vlmMapRef
    )
        throws DatabaseException
    {
        return new Resource(
            resUuidRef,
            objProtRef,
            resDfnRef,
            nodeRef,
            initFlagsRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef,
            rscConnMapRef,
            vlmMapRef
        );
    }

    /**
     * Creates a new {@link ResourceDefinition} without persisting it to the database
     * @throws DatabaseException
     */
    public static ResourceDefinition createResourceDefinition(
        UUID resDfnUuidRef,
        ObjectProtection resDfnObjProtRef,
        ResourceName resNameRef,
        byte[] extName,
        long flagValueRef,
        List<DeviceLayerKind> layerStackRef,
        ResourceDefinitionGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, VolumeDefinition> vlmDfnMapRef,
        Map<NodeName, Resource> rscMapRef,
        Map<SnapshotName, SnapshotDefinition> snapshotDfnMapRef,
        Map<Pair<DeviceLayerKind, String>, RscDfnLayerObject> layerDataMapRef,
        ResourceGroup rscGrpRef
    )
        throws DatabaseException
    {
        return new ResourceDefinition(
            resDfnUuidRef,
            resDfnObjProtRef,
            resNameRef,
            extName,
            flagValueRef,
            layerStackRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef,
            vlmDfnMapRef,
            rscMapRef,
            snapshotDfnMapRef,
            layerDataMapRef,
            rscGrpRef
        );
    }

    /**
     * Creates a new {@link StorPool} without persisting it to the database
     * @throws DatabaseException
     */
    public static StorPool createStorPool(
        UUID uuidRef,
        Node nodeRef,
        StorPoolDefinition spddRef,
        DeviceProviderKind lvmRef,
        FreeSpaceMgr fsmRef,
        StorPoolGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<String, VlmProviderObject> volumeMapRef
    )
        throws DatabaseException
    {
        return new StorPool(
            uuidRef,
            nodeRef,
            spddRef,
            lvmRef,
            fsmRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef,
            volumeMapRef
        );
    }

    /**
     * Creates a new {@link StorPoolDefinition} without persisting it to the database
     * @throws DatabaseException
     */
    public static StorPoolDefinition createStorPoolDefinition(
        UUID uuidRef,
        ObjectProtection objProtRef,
        StorPoolName spNameRef,
        StorPoolDefinitionGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<NodeName, StorPool> storPoolsMapRef
    )
        throws DatabaseException
    {
        return new StorPoolDefinition(
            uuidRef,
            objProtRef,
            spNameRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef,
            storPoolsMapRef
        );
    }

    /**
     * Creates a new {@link VolumeConnection} without persisting it to the database
     * @throws DatabaseException
     */
    public static VolumeConnection createVolumeConnection(
        UUID uuidRef,
        Volume volSrcRef,
        Volume volDstRef,
        VolumeConnectionGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        return new VolumeConnection(
            uuidRef,
            volSrcRef,
            volDstRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
    }

    /**
     * Creates a new {@link Volume} without persisting it to the database
     * @throws DatabaseException
     */
    public static Volume createVolume(
        UUID uuidRef,
        Resource resRef,
        VolumeDefinition volDfnRef,
        long flagValueRef,
        VolumeGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<Volume.Key, VolumeConnection> vlmConnsMapRef
    )
        throws DatabaseException
    {
        return new Volume(
            uuidRef,
            resRef,
            volDfnRef,
            flagValueRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef,
            vlmConnsMapRef
        );
    }

    /**
     * Creates a new {@link VolumeDefinition} without persisting it to the database
     * @throws MdException
     * @throws DatabaseException
     */
    public static VolumeDefinition VolumeDefinition(
        UUID uuidRef,
        ResourceDefinition resDfnRef,
        VolumeNumber volNrRef,
        long volSizeRef,
        long flagValueRef,
        VolumeDefinitionGenericDbDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<String, Volume> vlmMapRef,
        Map<Pair<DeviceLayerKind, String>, VlmDfnLayerObject> layerDataMapRef
    )
        throws DatabaseException, MdException
    {
        return new VolumeDefinition(
            uuidRef,
            resDfnRef,
            volNrRef,
            volSizeRef,
            flagValueRef,
            driverRef,
            propsContainerFactoryRef,
            transObjFactoryRef,
            transMgrProviderRef,
            vlmMapRef,
            layerDataMapRef
        );
    }

    private TestFactory()
    {
    }

}
