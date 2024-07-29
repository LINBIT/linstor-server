package com.linbit.linstor.dbcp.migration;

import com.linbit.ImplementationError;
import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.utils.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

@Migration(
    version = "2021.04.29.12.00",
    description = "Fix duplicated restored layer data"
)
public class Migration_2021_04_29_FixDuplicatedRestoredLayerData extends LinstorMigration
{
    private static final String LAYER_RESOURCE_ID = "LAYER_RESOURCE_ID";
    private static final String NODE_NAME = "NODE_NAME";
    private static final String RESOURCE_NAME = "RESOURCE_NAME";
    private static final String SNAPSHOT_NAME = "SNAPSHOT_NAME";
    private static final String KIND = "LAYER_RESOURCE_KIND";
    private static final String RSC_SUFFIX = "LAYER_RESOURCE_SUFFIX";
    /*
     * 1.12.0 introduced a bug where restoring a snapshot into a new resource by accident creates too many layer-data.
     * The resource itself will work as expected, but when trying to remove the resource, the additionally created
     * layer-data will cause constraint violation exceptions as they still have foreign keys to the resource.
     */

    @Override
    protected void migrate(Connection dbCon, DbProduct dbProductRef) throws Exception
    {
        try
            (
                PreparedStatement select = dbCon.prepareStatement(
                    " SELECT LRI." +
                        StringUtils.join(
                            ", LRI.",
                            LAYER_RESOURCE_ID,
                            NODE_NAME,
                            RESOURCE_NAME,
                            SNAPSHOT_NAME,
                            KIND,
                            RSC_SUFFIX
                        ) +
                        " FROM LAYER_RESOURCE_IDS AS LRI"
                );
                ResultSet rs = select.executeQuery();

                PreparedStatement deleteLayerRscId = dbCon.prepareStatement(
                    "DELETE FROM LAYER_RESOURCE_IDS " +
                        " WHERE " + LAYER_RESOURCE_ID + " = ?"
                );

                PreparedStatement deleteCacheVlms = dbCon.prepareStatement(
                    "DELETE FROM LAYER_CACHE_VOLUMES " +
                        " WHERE " + LAYER_RESOURCE_ID + " = ?"
                );
                PreparedStatement deleteDrbdRscs = dbCon.prepareStatement(
                    "DELETE FROM LAYER_DRBD_RESOURCES " +
                        " WHERE " + LAYER_RESOURCE_ID + " = ?"
                );
                PreparedStatement deleteDrbdVlms = dbCon.prepareStatement(
                    "DELETE FROM LAYER_DRBD_VOLUMES " +
                        " WHERE " + LAYER_RESOURCE_ID + " = ?"
                );
                PreparedStatement deleteLuksVlms = dbCon.prepareStatement(
                    "DELETE FROM LAYER_LUKS_VOLUMES " +
                        " WHERE " + LAYER_RESOURCE_ID + " = ?"
                );
                PreparedStatement deleteOpenflexVlms = dbCon.prepareStatement(
                    "DELETE FROM LAYER_OPENFLEX_VOLUMES " +
                        " WHERE " + LAYER_RESOURCE_ID + " = ?"
                );
                PreparedStatement deleteStorageVlms = dbCon.prepareStatement(
                    "DELETE FROM LAYER_STORAGE_VOLUMES " +
                        " WHERE " + LAYER_RESOURCE_ID + " = ?"
                );
                PreparedStatement deleteWriteCacheVlms = dbCon.prepareStatement(
                    "DELETE FROM LAYER_WRITECACHE_VOLUMES " +
                        " WHERE " + LAYER_RESOURCE_ID + " = ?"
                );
            )
        {
            HashMap<LriKey, Integer> lastIds = new HashMap<>();
            while (rs.next())
            {
                String kind = rs.getString(KIND);
                LriKey key = new LriKey(
                    rs.getString(NODE_NAME),
                    rs.getString(RESOURCE_NAME),
                    rs.getString(SNAPSHOT_NAME),
                    kind,
                    rs.getString(RSC_SUFFIX)
                );
                Integer lastId = lastIds.put(key, rs.getInt(LAYER_RESOURCE_ID));
                if (lastId != null)
                {
                    ArrayList<PreparedStatement> deleteStmts = new ArrayList<>();
                    switch (kind)
                    {
                        case "CACHE":
                            deleteStmts.add(deleteCacheVlms);
                            break;
                        case "DRBD":
                            deleteStmts.add(deleteDrbdVlms);
                            deleteStmts.add(deleteDrbdRscs);
                            break;
                        case "LUKS":
                            deleteStmts.add(deleteLuksVlms);
                            break;
                        case "OPENFLEX":
                            deleteStmts.add(deleteOpenflexVlms);
                            break;
                        case "STORAGE":
                            deleteStmts.add(deleteStorageVlms);
                            break;
                        case "WRITECACHE":
                            deleteStmts.add(deleteWriteCacheVlms);
                            break;
                        default:
                            throw new ImplementationError("Unknown kind: " + kind);
                    }
                    deleteStmts.add(deleteLayerRscId);
                    for (PreparedStatement ps : deleteStmts)
                    {
                        ps.setInt(1, lastId);
                        ps.execute();
                    }
                }
            }
        }
    }

    private static class LriKey
    {
        @Nullable String nodeName;
        @Nullable String rscName;
        @Nullable String snapName;
        @Nullable String kind;
        @Nullable String rscSuffix;

        LriKey(
            @Nullable String nodeNameRef,
            @Nullable String rscNameRef,
            @Nullable String snapNameRef,
            @Nullable String kindRef,
            @Nullable String rscSuffixRef
        )
        {
            nodeName = nodeNameRef;
            rscName = rscNameRef;
            snapName = snapNameRef;
            kind = kindRef;
            rscSuffix = rscSuffixRef;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((kind == null) ? 0 : kind.hashCode());
            result = prime * result + ((nodeName == null) ? 0 : nodeName.hashCode());
            result = prime * result + ((rscName == null) ? 0 : rscName.hashCode());
            result = prime * result + ((snapName == null) ? 0 : snapName.hashCode());
            result = prime * result + ((rscSuffix == null) ? 0 : rscSuffix.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            LriKey other = (LriKey) obj;

            return Objects.equals(nodeName, other.nodeName) &&
                Objects.equals(rscName, other.rscName) &&
                Objects.equals(snapName, other.snapName) &&
                Objects.equals(kind, other.kind) &&
                Objects.equals(rscSuffix, other.rscSuffix);
        }

    }
}
