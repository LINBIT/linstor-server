package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclCtrlDatabaseDriver.SecObjProtAclParent;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecAclMap.ACCESS_TYPE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecAclMap.OBJECT_PATH;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecAclMap.ROLE_NAME;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
public final class SecObjectProtectionAclDbDriver
    extends AbsDatabaseDriver<AccessControlEntry, Void, SecObjProtAclParent>
    implements SecObjProtAclCtrlDatabaseDriver
{
    private final SingleColumnDatabaseDriver<AccessControlEntry, AccessType> accessTypeDriver;

    private int loadedCount = 0;

    @Inject
    public SecObjectProtectionAclDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_ACL_MAP, dbEngineRef);

        setColumnSetter(OBJECT_PATH, acEntry -> acEntry.objPath);
        setColumnSetter(ROLE_NAME, acEntry -> acEntry.subjectRole.name.value);
        setColumnSetter(ACCESS_TYPE, acEntry -> acEntry.access.getAccessMask());

        switch (getDbType())
        {
            case SQL:// fall-through
            case K8S_CRD:
                setColumnSetter(ACCESS_TYPE, acEntry -> acEntry.access.getAccessMask());
                accessTypeDriver = generateSingleColumnDriver(ACCESS_TYPE, this::getId, AccessType::getAccessMask);
                break;
            default:
                throw new ImplementationError("Unexpected db type: " + getDbType());
        }
    }

    @Override
    public SingleColumnDatabaseDriver<AccessControlEntry, AccessType> getAccessTypeDriver()
    {
        return accessTypeDriver;
    }

    @Override
    protected @Nullable Pair<AccessControlEntry, Void> load(RawParameters rawRef, SecObjProtAclParent parentRef)
        throws IllegalArgumentException, InvalidNameException, DatabaseException
    {
        String objPath = rawRef.get(OBJECT_PATH);
        Map<RoleName, AccessControlEntry> parentAclsBackingMap = parentRef.getParentAcl(objPath);
        Role role = parentRef.getRole(rawRef.build(ROLE_NAME, RoleName::new));
        parentAclsBackingMap.put(
            role.name,
            new AccessControlEntry(
                objPath,
                role,
                rawRef.<@Nullable Short, @Nullable AccessType, IllegalArgumentException>buildParsed(
                    ACCESS_TYPE,
                    AccessType::get
                )
            )
        );
        loadedCount++;

        return null;
    }

    @Override
    protected int getLoadedCount(Map<AccessControlEntry, Void> ignoredEmptyMap)
    {
        return loadedCount;
    }

    @Override
    protected String getId(AccessControlEntry dataRef) throws AccessDeniedException
    {
        return "AccessCtrlEntry: " + dataRef.objPath + ", Role: " + dataRef.subjectRole.name.displayValue +
            ", Access: " + dataRef.access.name();
    }
}
