package com.linbit.linstor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.UuidUtils;

public class VolumeConnectionDataDerbyDriver implements VolumeConnectionDataDatabaseDriver
{
    private static final String TBL_VOL_CON_DFN = DerbyConstants.TBL_VOLUME_CONNECTIONS;

    private static final String UUID = DerbyConstants.UUID;
    private static final String NODE_SRC = DerbyConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DerbyConstants.NODE_NAME_DST;
    private static final String RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String VOL_NR = DerbyConstants.VLM_NR;

    private static final String SELECT =
        " SELECT " + UUID + ", " + NODE_SRC + ", " + NODE_DST + ", " +
                     RES_NAME + ", " + VOL_NR +
        " FROM "  + TBL_VOL_CON_DFN +
        " WHERE " + NODE_SRC + " = ? AND " +
                    NODE_DST + " = ? AND " +
                    RES_NAME + " = ? AND " +
                    VOL_NR + " = ?";
    private static final String SELECT_BY_VOL_SRC_OR_DST =
        " SELECT " + UUID + ", " + NODE_SRC + ", " + NODE_DST + ", " +
                     RES_NAME + ", " + VOL_NR +
        " FROM "  + TBL_VOL_CON_DFN +
        " WHERE (" +
                        NODE_SRC + " = ? OR " +
                        NODE_DST + " = ? " +
               ") AND " +
                    RES_NAME + " = ? AND " +
                    VOL_NR + " = ?";

    private static final String INSERT =
        " INSERT INTO " + TBL_VOL_CON_DFN +
        " (" +
            UUID + ", " + NODE_SRC + ", " + NODE_DST + ", " +
            RES_NAME + ", " + VOL_NR +
        ") VALUES (?, ?, ?, ?, ?)";

    private static final String DELETE =
        " DELETE FROM " + TBL_VOL_CON_DFN +
         " WHERE " + NODE_SRC + " = ? AND " +
                     NODE_DST + " = ? AND " +
                     RES_NAME + " = ? AND " +
                     VOL_NR + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private NodeDataDerbyDriver nodeDataDerbyDriver;
    private ResourceDefinitionDataDerbyDriver resourceDefinitionDataDerbyDriver;
    private ResourceDataDerbyDriver resourceDataDerbyDriver;
    private VolumeDefinitionDataDerbyDriver volumeDefinitionDataDerbyDriver;
    private VolumeDataDerbyDriver volumeDataDerbyDriver;

    public VolumeConnectionDataDerbyDriver(
        AccessContext accCtx,
        ErrorReporter errorReporterRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
    }

    public void initialize(
        NodeDataDerbyDriver nodeDataDerbyDriverRef,
        ResourceDefinitionDataDerbyDriver resourceDefinitionDataDerbyDriverRef,
        ResourceDataDerbyDriver resourceDataDerbyDriverRef,
        VolumeDefinitionDataDerbyDriver volumeDefinitionDataDerbyDriverRef,
        VolumeDataDerbyDriver volumeDataDerbyDriverRef
    )
    {
        nodeDataDerbyDriver = nodeDataDerbyDriverRef;
        resourceDefinitionDataDerbyDriver = resourceDefinitionDataDerbyDriverRef;
        resourceDataDerbyDriver = resourceDataDerbyDriverRef;
        volumeDefinitionDataDerbyDriver = volumeDefinitionDataDerbyDriverRef;
        volumeDataDerbyDriver = volumeDataDerbyDriverRef;
    }

    @Override
    public VolumeConnectionData load(
        Volume sourceVolume,
        Volume targetVolume,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        ResourceDefinition resDfn = sourceVolume.getResource().getDefinition();
        VolumeDefinition volDfn = sourceVolume.getVolumeDefinition();
        if (volDfn != targetVolume.getVolumeDefinition() ||
            resDfn != targetVolume.getResource().getDefinition())
        {
            throw new ImplementationError(
                String.format(
                    "Failed to load VolumeConnection between unrelated volumes. %s %s",
                    getVolumeTraceId(sourceVolume),
                    getVolumeTraceId(targetVolume)
                ),
                null
            );
        }

        errorReporter.logTrace(
            "Loading VolumeConnection %s",
            getId(sourceVolume, targetVolume)
        );

        Node sourceNode = sourceVolume.getResource().getAssignedNode();
        Node targetNode = targetVolume.getResource().getAssignedNode();

        VolumeConnectionData ret = cacheGet(
            sourceNode,
            targetNode,
            resDfn,
            volDfn
        );

        if (ret == null)
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT))
            {
                stmt.setString(1, sourceNode.getName().value);
                stmt.setString(2, targetNode.getName().value);
                stmt.setString(3, resDfn.getName().value);
                stmt.setInt(4, volDfn.getVolumeNumber().value);

                try (ResultSet resultSet = stmt.executeQuery())
                {
                    if (resultSet.next())
                    {
                        ret = restoreVolumeConnectionData(
                            resultSet,
                            transMgr
                        );
                        errorReporter.logTrace(
                            "VolumeConnection loaded %s",
                            getId(ret)
                        );
                    }
                    else
                    if (logWarnIfNotExists)
                    {
                        errorReporter.logWarning(
                            "VolumeConnection not found in the DB %s",
                            getId(sourceVolume, targetVolume)
                        );
                    }
                }
            }
        }
        return ret;
    }

    @Override
    public List<VolumeConnectionData> loadAllByVolume(
        Volume volume,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace(
            "Loading all VolumeConnections for Volume %s",
            getVolumeTraceId(volume)
        );

        List<VolumeConnectionData> connections = new ArrayList<>();
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_BY_VOL_SRC_OR_DST))
        {
            NodeName nodeName = volume.getResource().getAssignedNode().getName();
            stmt.setString(1, nodeName.value);
            stmt.setString(2, nodeName.value);
            stmt.setString(3, volume.getResourceDefinition().getName().value);
            stmt.setInt(4, volume.getVolumeDefinition().getVolumeNumber().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    VolumeConnectionData conDfn = restoreVolumeConnectionData(
                        resultSet,
                        transMgr
                    );
                    connections.add(conDfn);
                }
            }
        }

        errorReporter.logTrace(
            "%d VolumeConnections loaded for Resource %s",
            connections.size(),
            getVolumeDebugId(volume)
        );
        return connections;
    }

    private VolumeConnectionData restoreVolumeConnectionData(
        ResultSet resultSet,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        NodeName sourceNodeName = null;
        NodeName targetNodeName = null;
        ResourceName resourceName = null;
        VolumeNumber volNr = null;
        try
        {
            sourceNodeName = new NodeName(resultSet.getString(NODE_SRC));
            targetNodeName = new NodeName(resultSet.getString(NODE_DST));
            resourceName = new ResourceName(resultSet.getString(RES_NAME));
            volNr = new VolumeNumber(resultSet.getInt(VOL_NR));
        }
        catch (InvalidNameException invalidNameExc)
        {
            String col;
            String format = "A %s of a stored VolumeConnection in table %s could not be restored. ";
            if (sourceNodeName == null)
            {
                format += "(invalid SourceNodeName=%s, TargetNodeName=%s, ResName=%s)";
                col = "SourceNodeName";
            }
            else
            if (targetNodeName == null)
            {
                format += "(SourceNodeName=%s, invalid TargetNodeName=%s, ResName=%s)";
                col = "TargetNodeName";
            }
            else
            {
                format += "(SourceNodeName=%s, TargetNodeName=%s, invalid ResName=%s)";
                col = "ResName";
            }
            throw new LinStorSqlRuntimeException(
                String.format(
                    format,
                    col,
                    TBL_VOL_CON_DFN,
                    resultSet.getString(NODE_SRC),
                    resultSet.getString(NODE_DST),
                    resultSet.getString(RES_NAME),
                    resultSet.getInt(VOL_NR)
                ),
                invalidNameExc
            );
        }
        catch (ValueOutOfRangeException invalidNameExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "A VolumeNumber of a stored VolumeConnection in table %s could not be restored. " +
                        "(SourceNodeName=%s, TargetNodeName=%s, ResName=%s, invalid VolNr=%d)",
                    TBL_VOL_CON_DFN,
                    resultSet.getString(NODE_SRC),
                    resultSet.getString(NODE_DST),
                    resultSet.getString(RES_NAME),
                    resultSet.getInt(VOL_NR)
                ),
                invalidNameExc
            );
        }

        Node sourceNode = nodeDataDerbyDriver.load(sourceNodeName, true, transMgr);
        Node targetNode = nodeDataDerbyDriver.load(targetNodeName, true, transMgr);

        ResourceDefinition resourceDefinition = resourceDefinitionDataDerbyDriver.load(
            resourceName,
            true,
            transMgr
        );
        VolumeDefinition volumeDfn = volumeDefinitionDataDerbyDriver.load(resourceDefinition, volNr, true, transMgr);

        Resource sourceResource = resourceDataDerbyDriver.load(sourceNode, resourceName, true, transMgr);
        Resource targetResource = resourceDataDerbyDriver.load(targetNode, resourceName, true, transMgr);

        Volume sourceVolume = volumeDataDerbyDriver.load(sourceResource, volumeDfn, true, transMgr);
        Volume targetVolume = volumeDataDerbyDriver.load(targetResource, volumeDfn, true, transMgr);

        VolumeConnectionData ret = null;
        try
        {
            ret = cacheGet(
                sourceNode,
                targetNode,
                sourceVolume.getResourceDefinition(),
                sourceVolume.getVolumeDefinition()
            );
            if (ret == null)
            {
                UUID uuid = UuidUtils.asUuid(resultSet.getBytes(UUID));

                ret = new VolumeConnectionData(
                    uuid,
                    dbCtx,
                    sourceVolume,
                    targetVolume,
                    transMgr
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    @Override
    public void create(VolumeConnectionData conDfnData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating VolumeConnection %s", getId(conDfnData));

        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT))
        {
            Volume sourceVolume = conDfnData.getSourceVolume(dbCtx);
            Volume targetVolume = conDfnData.getTargetVolume(dbCtx);

            stmt.setBytes(1, UuidUtils.asByteArray(conDfnData.getUuid()));
            stmt.setString(2, sourceVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(3, targetVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(4, sourceVolume.getResourceDefinition().getName().value);
            stmt.setInt(5, sourceVolume.getVolumeDefinition().getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeConnection created %s", getId(conDfnData));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    public void delete(VolumeConnectionData conDfnData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting VolumeConnection %s", getId(conDfnData));

        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(DELETE))
        {
            Volume sourceVolume = conDfnData.getSourceVolume(dbCtx);
            Volume targetVolume = conDfnData.getTargetVolume(dbCtx);

            stmt.setString(1, sourceVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(2, targetVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(3, sourceVolume.getResourceDefinition().getName().value);
            stmt.setInt(4, sourceVolume.getVolumeDefinition().getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeConnection deleted %s", getId(conDfnData));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    private VolumeConnectionData cacheGet(
        Node nodeSrc,
        Node nodeDst,
        ResourceDefinition resDfn,
        VolumeDefinition volDfn
    )
    {
        VolumeConnectionData ret = null;
        try
        {
            Resource resSrc = resDfn.getResource(dbCtx, nodeSrc.getName());
            Resource resDst = resDfn.getResource(dbCtx, nodeDst.getName());
            if (resSrc != null && resDst != null)
            {
                Volume volSrc = resSrc.getVolume(volDfn.getVolumeNumber());
                Volume volDst = resDst.getVolume(volDfn.getVolumeNumber());

                if (volSrc != null && volDst != null)
                {
                    ret = (VolumeConnectionData) volSrc.getVolumeConnection(dbCtx, volDst);
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    private String getId(VolumeConnectionData volConData)
    {
        String id = null;
        try
        {
            Volume sourceVolume = volConData.getSourceVolume(dbCtx);
            Volume targetVolume = volConData.getTargetVolume(dbCtx);
            id = getId(
                sourceVolume.getResource().getAssignedNode().getName().displayValue,
                targetVolume.getResource().getAssignedNode().getName().displayValue,
                sourceVolume.getResourceDefinition().getName().displayValue,
                sourceVolume.getVolumeDefinition().getVolumeNumber().value
            );
        }
        catch (AccessDeniedException accDeniedException)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedException);
        }
        return id;
    }

    private String getId(Volume sourceVolume, Volume targetVolume)
    {
        return getId(
            sourceVolume.getResource().getAssignedNode().getName().displayValue,
            targetVolume.getResource().getAssignedNode().getName().displayValue,
            sourceVolume.getResource().getDefinition().getName().displayValue,
            sourceVolume.getVolumeDefinition().getVolumeNumber().value
        );
    }

    private String getId(
        String sourceName,
        String targetName,
        String resName,
        int volNr
    )
    {
        return "(SourceNode=" + sourceName +
            " TargetNode=" + targetName +
            " ResName=" + resName +
            " VolNr=" + volNr + ")";
    }

    private String getVolumeTraceId(Volume volume)
    {
        return getVolumeId(
            volume.getResource().getAssignedNode().getName().value,
            volume.getResourceDefinition().getName().value,
            volume.getVolumeDefinition().getVolumeNumber().value
        );
    }

    private String getVolumeDebugId(Volume volume)
    {
        return getVolumeId(
            volume.getResource().getAssignedNode().getName().displayValue,
            volume.getResourceDefinition().getName().displayValue,
            volume.getVolumeDefinition().getVolumeNumber().value
        );
    }

    private String getVolumeId(String nodeName, String resName, int volNr)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + " VolNr=" + volNr + ")";
    }
}
