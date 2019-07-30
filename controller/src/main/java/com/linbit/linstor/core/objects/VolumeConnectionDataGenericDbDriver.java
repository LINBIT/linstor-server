package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnectionData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.StringUtils;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class VolumeConnectionDataGenericDbDriver implements VolumeConnectionDataDatabaseDriver
{
    private static final String TBL_VOL_CON_DFN = DbConstants.TBL_VOLUME_CONNECTIONS;
    private static final String UUID = DbConstants.UUID;
    private static final String NODE_SRC = DbConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DbConstants.NODE_NAME_DST;
    private static final String RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String VOL_NR = DbConstants.VLM_NR;
    private static final String[] VOL_CON_FIELDS = {
        UUID,
        NODE_SRC,
        NODE_DST,
        RES_NAME,
        VOL_NR
    };

    private static final String SELECT_ALL =
        " SELECT " + StringUtils.join(", ", VOL_CON_FIELDS) +
        " FROM "  + TBL_VOL_CON_DFN;

    private static final String INSERT =
        " INSERT INTO " + TBL_VOL_CON_DFN +
        " (" + StringUtils.join(", ", VOL_CON_FIELDS) + ")" +
        "VALUES ( " + StringUtils.repeat("?", ", ", VOL_CON_FIELDS.length) + " )";

    private static final String DELETE =
        " DELETE FROM " + TBL_VOL_CON_DFN +
         " WHERE " + NODE_SRC + " = ? AND " +
                     NODE_DST + " = ? AND " +
                     RES_NAME + " = ? AND " +
                     VOL_NR + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeConnectionDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public List<VolumeConnectionData> loadAll(
        Map<Triple<NodeName, ResourceName, VolumeNumber>, ? extends Volume> vlmMap
    )
        throws DatabaseException
    {
        errorReporter.logTrace("Loading all VolumeConnections");
        List<VolumeConnectionData> vlmConns = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    NodeName nodeNameSrc = new NodeName(resultSet.getString(NODE_SRC));
                    NodeName nodeNameDst = new NodeName(resultSet.getString(NODE_DST));
                    ResourceName rscName = new ResourceName(resultSet.getString(RES_NAME));
                    VolumeNumber vlmNr = new VolumeNumber(resultSet.getInt(VOL_NR));

                    VolumeConnectionData vlmConn = restoreVolumeConnectionData(
                        resultSet,
                        vlmMap.get(new Triple<>(nodeNameSrc, rscName, vlmNr)),
                        vlmMap.get(new Triple<>(nodeNameDst, rscName, vlmNr))
                    );
                    vlmConns.add(vlmConn);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(
                TBL_VOL_CON_DFN + " contains invalid name: " + exc.invalidName,
                exc
            );
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError(
                TBL_VOL_CON_DFN + " contains invalid volume number",
                exc
            );
        }
        return vlmConns;
    }

    private VolumeConnectionData restoreVolumeConnectionData(
        ResultSet resultSet,
        Volume sourceVolume,
        Volume targetVolume
    )
        throws DatabaseException
    {
        try
        {
            return new VolumeConnectionData(
                java.util.UUID.fromString(resultSet.getString(UUID)),
                sourceVolume,
                targetVolume,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(VolumeConnectionData conDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Creating VolumeConnection %s", getId(conDfnData));

        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            Volume sourceVolume = conDfnData.getSourceVolume(dbCtx);
            Volume targetVolume = conDfnData.getTargetVolume(dbCtx);

            stmt.setString(1, conDfnData.getUuid().toString());
            stmt.setString(2, sourceVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(3, targetVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(4, sourceVolume.getResourceDefinition().getName().value);
            stmt.setInt(5, sourceVolume.getVolumeDefinition().getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeConnection created %s", getId(conDfnData));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(VolumeConnectionData conDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting VolumeConnection %s", getId(conDfnData));

        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
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
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accDeniedExc);
        }
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
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
            DatabaseLoader.handleAccessDeniedException(accDeniedException);
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

    private String getVolumeId(String nodeName, String resName, int volNr)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + " VolNr=" + volNr + ")";
    }
}
