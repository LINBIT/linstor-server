package com.linbit.linstor;

import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Node extends TransactionObject, DbgInstanceUuid, Comparable<Node>, ProtectedObject
{
    UUID getUuid();

    NodeName getName();

    NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName)
        throws AccessDeniedException;

    Iterator<NetInterface> iterateNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<NetInterface> streamNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException;

    Resource getResource(AccessContext accCtx, ResourceName name)
        throws AccessDeniedException;

    void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    NodeConnection getNodeConnection(AccessContext accCtx, Node otherNode)
        throws AccessDeniedException;

    void setNodeConnection(AccessContext accCtx, NodeConnection nodeConnection)
        throws AccessDeniedException;

    void removeNodeConnection(AccessContext accCtx, NodeConnection nodeConnection)
        throws AccessDeniedException;

    int getResourceCount();

    Iterator<Resource> iterateResources(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<Resource> streamResources(AccessContext accCtx)
        throws AccessDeniedException;

    void addSnapshot(AccessContext accCtx, Snapshot snapshot)
        throws AccessDeniedException;

    void removeSnapshot(SnapshotData snapshotData);

    boolean hasSnapshots(AccessContext accCtx)
        throws AccessDeniedException;

    Collection<Snapshot> getInProgressSnapshots(AccessContext accCtx)
        throws AccessDeniedException;

    Collection<Snapshot> getSnapshots(AccessContext accCtx)
        throws AccessDeniedException;

    void copyStorPoolMap(AccessContext accCtx, Map<? super StorPoolName, ? super StorPool> dstMap)
        throws AccessDeniedException;

    int getStorPoolCount();

    StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
        throws AccessDeniedException;

    Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<StorPool> streamStorPools(AccessContext accCtx)
        throws AccessDeniedException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    NodeType getNodeType(AccessContext accCtx)
        throws AccessDeniedException;

    boolean hasNodeType(AccessContext accCtx, NodeType reqType)
        throws AccessDeniedException;

    StateFlags<NodeFlag> getFlags();

    void setPeer(AccessContext accCtx, Peer peer) throws AccessDeniedException;

    Peer getPeer(AccessContext accCtx) throws AccessDeniedException;

    NetInterface getActiveStltConn(AccessContext accCtx)
        throws AccessDeniedException;

    void setActiveStltConn(AccessContext accCtx, NetInterface netIf)
        throws AccessDeniedException, SQLException;

    /**
     * @return true iff this is the initial connection attempt
     */
    boolean connectionEstablished();

    void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    boolean isDeleted();

    NodeApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    enum NodeType implements Flags
    {
        CONTROLLER(1, Collections.emptyList()),
        SATELLITE(
            2,
            Arrays.asList(
                DeviceProviderKind.DISKLESS,
                DeviceProviderKind.LVM,
                DeviceProviderKind.LVM_THIN,
                DeviceProviderKind.SWORDFISH_INITIATOR,
                DeviceProviderKind.ZFS,
                DeviceProviderKind.ZFS_THIN,
                DeviceProviderKind.FILE,
                DeviceProviderKind.FILE_THIN
            )
        ),
        COMBINED(
            3,
            Arrays.asList(
                DeviceProviderKind.DISKLESS,
                DeviceProviderKind.LVM,
                DeviceProviderKind.LVM_THIN,
                DeviceProviderKind.SWORDFISH_INITIATOR,
                DeviceProviderKind.ZFS,
                DeviceProviderKind.ZFS_THIN,
                DeviceProviderKind.FILE,
                DeviceProviderKind.FILE_THIN
            )
        ),
        AUXILIARY(4, Collections.emptyList()),
        SWORDFISH_TARGET(
            5,
            Arrays.asList(
                DeviceProviderKind.SWORDFISH_TARGET
            )
        );

        private final int flag;
        private final List<DeviceProviderKind> allowedKindClasses;

        NodeType(int flagValue, List<DeviceProviderKind> allowedKindClassesRef)
        {

            flag = flagValue;
            allowedKindClasses = Collections.unmodifiableList(allowedKindClassesRef);
        }

        @Override
        public long getFlagValue()
        {
            return flag;
        }

        public static NodeType getByValue(long value)
        {
            NodeType ret = null;
            for (NodeType type : NodeType.values())
            {
                if (type.flag == value)
                {
                    ret = type;
                    break;
                }
            }
            return ret;
        }

        public static NodeType valueOfIgnoreCase(String string, NodeType defaultValue)
            throws IllegalArgumentException
        {
            NodeType ret = defaultValue;
            if (string != null)
            {
                NodeType val = valueOf(string.toUpperCase());
                if (val != null)
                {
                    ret = val;
                }
            }
            return ret;
        }

        public List<DeviceProviderKind> getAllowedKindClasses()
        {
            return allowedKindClasses;
        }

        public boolean isDeviceProviderKindAllowed(DeviceProviderKind kindRef)
        {
            return allowedKindClasses.contains(kindRef);
        }
    }

    enum NodeFlag implements Flags
    {
        DELETE(1L),
        QIGNORE(0x10000L);

        public final long flagValue;

        NodeFlag(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static NodeFlag[] valuesOfIgnoreCase(String string)
        {
            NodeFlag[] flags;
            if (string == null)
            {
                flags = new NodeFlag[0];
            }
            else
            {
                String[] split = string.split(",");
                flags = new NodeFlag[split.length];

                for (int idx = 0; idx < split.length; idx++)
                {
                    flags[idx] = NodeFlag.valueOf(split[idx].toUpperCase().trim());
                }
            }
            return flags;
        }

        public static NodeFlag[] restoreFlags(long nodeFlags)
        {
            List<NodeFlag> flagList = new ArrayList<>();
            for (NodeFlag flag : NodeFlag.values())
            {
                if ((nodeFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new NodeFlag[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(NodeFlag.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(NodeFlag.class, listFlags);
        }
    }

    interface NodeApi
    {
        String getName();
        String getType();
        UUID getUuid();
        Peer.ConnectionStatus connectionStatus();
        Map<String, String> getProps();
        long getFlags();
        List<NetInterface.NetInterfaceApi> getNetInterfaces();
        NetInterfaceApi getActiveStltConn();
    }

    interface InitMaps
    {
        Map<ResourceName, Resource> getRscMap();
        Map<SnapshotDefinition.Key, Snapshot> getSnapshotMap();
        Map<NetInterfaceName, NetInterface> getNetIfMap();
        Map<StorPoolName, StorPool> getStorPoolMap();
        Map<NodeName, NodeConnection> getNodeConnMap();
    }
}
