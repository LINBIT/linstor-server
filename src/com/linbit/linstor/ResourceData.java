package com.linbit.linstor;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import javax.inject.Provider;

import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_NAME;

/**
 * Representation of a resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceData extends BaseTransactionObject implements Resource
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    // Connections to the peer resources
    private final TransactionMap<Resource, ResourceConnection> resourceConnections;

    // List of volumes of this resource
    private final TransactionMap<VolumeNumber, Volume> volumeMap;

    // Reference to the node this resource is assigned to
    private final Node assgNode;

    // State flags
    private final StateFlags<RscFlags> flags;

    // Access control for this resource
    private final ObjectProtection objProt;

    // DRBD node id for this resource
    private final NodeId resNodeId;

    // Properties container for this resource
    private final Props resourceProps;

    private final ResourceDataDatabaseDriver dbDriver;
    private final VolumeDataFactory volumeDataFactory;

    private final TransactionSimpleObject<ResourceData, Boolean> deleted;

    private boolean createPrimary = false;

    ResourceData(
        UUID objIdRef,
        AccessContext accCtx,
        ObjectProtection objProtRef,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        long initFlags,
        ResourceDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        VolumeDataFactory volumeDataFactoryRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
        throws SQLException, AccessDeniedException
    {
        super(transMgrProviderRef);
        dbDriver = dbDriverRef;
        volumeDataFactory = volumeDataFactoryRef;

        ErrorCheck.ctorNotNull(ResourceData.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(ResourceData.class, Node.class, nodeRef);
        resNodeId = nodeIdRef;
        resourceDfn = resDfnRef;
        assgNode = nodeRef;
        objId = objIdRef;
        dbgInstanceId = UUID.randomUUID();

        resourceConnections = transObjFactory.createTransactionMap(new HashMap<Resource, ResourceConnection>(), null);
        volumeMap = transObjFactory.createTransactionMap(new TreeMap<VolumeNumber, Volume>(), null);
        resourceProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                nodeRef.getName(),
                resDfnRef.getName()
            )
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);
        objProt = objProtRef;

        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            RscFlags.class,
            dbDriver.getStateFlagPersistence(),
            initFlags
        );

        transObjs = Arrays.asList(
            resourceDfn,
            assgNode,
            flags,
            objProt,
            resourceConnections,
            volumeMap,
            resourceProps,
            deleted
        );

        ((NodeData) nodeRef).addResource(accCtx, this);
        ((ResourceDefinitionData) resDfnRef).addResource(accCtx, this);
        activateTransMgr();
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, resourceProps);
    }

    @Override
    public ResourceDefinition getDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    @Override
    public synchronized void setResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException
    {
        checkDeleted();

        Resource sourceResource = resCon.getSourceResource(accCtx);
        Resource targetResource = resCon.getTargetResource(accCtx);

        sourceResource.getObjProt().requireAccess(accCtx, AccessType.USE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.USE);

        if (this == sourceResource)
        {
            resourceConnections.put(targetResource, resCon);
        }
        else
        {
            resourceConnections.put(sourceResource, resCon);
        }
    }

    @Override
    public synchronized void removeResourceConnection(AccessContext accCtx, ResourceConnection con)
        throws AccessDeniedException
    {
        checkDeleted();
        Resource sourceResource = con.getSourceResource(accCtx);
        Resource targetResource = con.getTargetResource(accCtx);

        sourceResource.getObjProt().requireAccess(accCtx, AccessType.USE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.USE);

        if (this == sourceResource)
        {
            resourceConnections.remove(targetResource);
        }
        else
        {
            resourceConnections.remove(sourceResource);
        }
    }

    @Override
    public ResourceConnection getResourceConnection(AccessContext accCtx, Resource otherResource)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceConnections.get(otherResource);
    }

    @Override
    public Volume getVolume(VolumeNumber volNr)
    {
        checkDeleted();
        return volumeMap.get(volNr);
    }

    public int getVolumeCount()
    {
        checkDeleted();
        return volumeMap.size();
    }

    synchronized Volume putVolume(AccessContext accCtx, Volume vol)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        return volumeMap.put(vol.getVolumeDefinition().getVolumeNumber(), vol);
    }

    synchronized void removeVolume(AccessContext accCtx, Volume vol)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        volumeMap.remove(vol.getVolumeDefinition().getVolumeNumber());
    }

    @Override
    public void adjustVolumes(AccessContext apiCtx, String defaultStorPoolName)
        throws InvalidNameException, LinStorException
    {
        checkDeleted();
        Iterator<VolumeDefinition> vlmDfns;
        try
        {
            vlmDfns = resourceDfn.iterateVolumeDfn(apiCtx);
            while (vlmDfns.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfns.next();
                PriorityProps prioProps = new PriorityProps(
                    this.getProps(apiCtx),
                    vlmDfn.getProps(apiCtx),
                    resourceDfn.getProps(apiCtx),
                    assgNode.getProps(apiCtx)
                );

                if (vlmDfn.getFlags().isSet(apiCtx, VlmDfnFlags.DELETE))
                {
                    // find corresponding volume (if exists) and also set DELETE flag
                }

                if (!volumeMap.containsKey(vlmDfn.getVolumeNumber()))
                {
                    // if vlm not yet deployed

                    StorPool storPool;
                    if (flags.isSet(apiCtx, RscFlags.DISKLESS))
                    {
                        storPool = assgNode.getDisklessStorPool(apiCtx);
                    }
                    else
                    {
                        String storPoolNameStr;
                        storPoolNameStr = LinStor.DISKLESS_STOR_POOL_NAME;
                        storPoolNameStr = prioProps.getProp(KEY_STOR_POOL_NAME);
                        if (storPoolNameStr == null || "".equals(storPoolNameStr))
                        {
                            storPoolNameStr = defaultStorPoolName;
                        }
                        storPool = assgNode.getStorPool(
                            apiCtx,
                            new StorPoolName(storPoolNameStr)
                        );
                        if (storPool == null)
                        {
                            throw new LinStorException(
                                "The configured storage pool '" + storPoolNameStr + "' could not be found."
                            );
                        }
                    }

                    volumeDataFactory.getInstance(
                        apiCtx,
                        this,
                        vlmDfn,
                        storPool,
                        null, // blockDevicePathRef,
                        null, // metaDiskPathRef,
                        null,
                        true,
                        true
                    );
                }
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(
                "AccCtx should have been privileged",
                accDeniedExc
            );
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError(
                "Hardcoded props key is invalid",
                invalidKeyExc
            );
        }
        catch (LinStorDataAlreadyExistsException | SQLException implExc)
        {
            throw new ImplementationError(implExc);
        }
    }

    @Override
    public Iterator<Volume> iterateVolumes()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(volumeMap.values()).iterator();
    }

    @Override
    public Stream<Volume> streamVolumes()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(volumeMap.values()).stream();
    }

    @Override
    public Node getAssignedNode()
    {
        checkDeleted();
        return assgNode;
    }

    @Override
    public NodeId getNodeId()
    {
        checkDeleted();
        return resNodeId;
    }

    @Override
    public StateFlags<RscFlags> getStateFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        getStateFlags().enableFlags(accCtx, RscFlags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            ((NodeData) assgNode).removeResource(accCtx, this);
            ((ResourceDefinitionData) resourceDfn).removeResource(accCtx, this);

            // preventing ConcurrentModificationException
            Collection<ResourceConnection> rscConnValues = new ArrayList<>(resourceConnections.values());
            for (ResourceConnection rscConn : rscConnValues)
            {
                rscConn.delete(accCtx);
            }

            // preventing ConcurrentModificationException
            Collection<Volume> vlmValues = new ArrayList<>(volumeMap.values());
            for (Volume vlm : vlmValues)
            {
                vlm.delete(accCtx);
            }

            resourceProps.delete();

            objProt.delete(accCtx);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted resource");
        }
    }

    public void setCreatePrimary()
    {
        createPrimary = true;
    }

    public void unsetCreatePrimary()
    {
        createPrimary = false;
    }

    @Override
    public boolean isCreatePrimary()
    {
        return createPrimary;
    }

    @Override
    public RscApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException
    {
        List<Volume.VlmApi> volumes = new ArrayList<>();
        Iterator<Volume> itVolumes = iterateVolumes();
        while (itVolumes.hasNext())
        {
            volumes.add(itVolumes.next().getApiData(accCtx));
        }
        return new RscPojo(
            getDefinition().getName().getDisplayName(),
            getAssignedNode().getName().getDisplayName(),
            getAssignedNode().getUuid(),
            getDefinition().getApiData(accCtx),
            getUuid(),
            getStateFlags().getFlagsBits(accCtx),
            getNodeId().value,
            getProps(accCtx).map(),
            volumes,
            null, // otherRscList
            fullSyncId,
            updateId
        );
    }

    @Override
    public String toString()
    {
        return "Node: '" + assgNode.getName() + "', " +
               "Rsc: '" + resourceDfn.getName() + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
