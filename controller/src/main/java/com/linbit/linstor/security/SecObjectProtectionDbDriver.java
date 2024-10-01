package com.linbit.linstor.security;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ObjProtMap;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtCtrlDatabaseDriver.SecObjProtInitObj;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtCtrlDatabaseDriver.SecObjProtParent;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecObjectProtection.CREATOR_IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecObjectProtection.OBJECT_PATH;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecObjectProtection.OWNER_ROLE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecObjectProtection.SECURITY_TYPE_NAME;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

@Singleton
public class SecObjectProtectionDbDriver
    extends AbsDatabaseDriver<ObjectProtection, SecObjProtInitObj, SecObjProtParent>
    implements SecObjProtCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final SecObjProtAclDatabaseDriver objProtAclDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<? extends TransactionMgr> transMgrProvider;

    private final SingleColumnDatabaseDriver<ObjectProtection, Role> roleDriver;
    private final SingleColumnDatabaseDriver<ObjectProtection, Identity> idDriver;
    private final SingleColumnDatabaseDriver<ObjectProtection, SecurityType> secTypeDriver;
    private final ObjProtMap objProtMap;

    @Inject
    public SecObjectProtectionDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        CoreModule.ObjProtMap objProtMapRef,
        DbEngine dbEngineRef,
        SecObjProtAclDatabaseDriver objProtAclDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_OBJECT_PROTECTION, dbEngineRef);
        dbCtx = dbCtxRef;
        objProtMap = objProtMapRef;
        objProtAclDriver = objProtAclDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(
            OBJECT_PATH,
            ObjectProtection::getObjectProtectionPath
        );
        setColumnSetter(
            CREATOR_IDENTITY_NAME,
            objProt -> objProt.getCreator().name.value
        );
        setColumnSetter(
            OWNER_ROLE_NAME,
            objProt -> objProt.getOwner().name.value
        );
        setColumnSetter(
            SECURITY_TYPE_NAME,
            objProt -> objProt.getSecurityType().name.value
        );

        idDriver = generateSingleColumnDriver(
            CREATOR_IDENTITY_NAME,
            ObjectProtection::getObjectProtectionPath,
            id -> id.name.value
        );
        roleDriver = generateSingleColumnDriver(
            OWNER_ROLE_NAME,
            ObjectProtection::getObjectProtectionPath,
            role -> role.name.value
        );
        secTypeDriver = generateSingleColumnDriver(
            SECURITY_TYPE_NAME,
            ObjectProtection::getObjectProtectionPath,
            secType -> secType.name.value
        );
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Role> getOwnerRoleDriver()
    {
        return roleDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Identity> getCreatorIdentityDriver()
    {
        return idDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver()
    {
        return secTypeDriver;
    }

    @Override
    public void delete(ObjectProtection dataRef) throws DatabaseException
    {
        super.delete(dataRef);
        // usually the entries in the corresponding maps should be deleted / cleaned up outside of the DB drivers.
        // however, since the objProt is created from many places (nodes, resources, rscDfn, rscGrp, extFiles, kvs, ...)
        // we make an exception and cleanup the objProtMap entry here
        synchronized (objProtMap)
        {
            objProtMap.remove(dataRef.getObjectProtectionPath());
        }
    }

    @Override
    protected Pair<ObjectProtection, SecObjProtInitObj> load(RawParameters rawRef, SecObjProtParent parentRef)
        throws InvalidNameException, AccessDeniedException
    {
        final String objPath = rawRef.get(OBJECT_PATH);
        final IdentityName creatorIdName = rawRef.build(CREATOR_IDENTITY_NAME, IdentityName::new);
        final RoleName ownerRoleName = rawRef.build(OWNER_ROLE_NAME, RoleName::new);
        final SecTypeName secTypeName = rawRef.build(SECURITY_TYPE_NAME, SecTypeName::new);

        final Identity creatorId = Objects.requireNonNull(
            parentRef.ids.get(creatorIdName),
            "Tried to load object protection (" + objPath + ") with missing creator identity: " + creatorIdName
        );
        final Role ownerRole = Objects.requireNonNull(
            parentRef.roles.get(ownerRoleName),
            "Tried to load object protection (" + objPath + ") with missing owner role: " + ownerRoleName
        );
        final SecurityType secType = Objects.requireNonNull(
            parentRef.secTypes.get(secTypeName),
            "Tried to load object protection (" + objPath + ") with missing security type: " + secTypeName
        );

        // create a new AccessContext to create the object protection since the ObjProt's constructor copies id, role
        // and secType from the passed accCtx
        final AccessContext impersonatedAccCtx = dbCtx.impersonate(creatorId, ownerRole, secType);

        Map<RoleName, AccessControlEntry> aclBackingMap = new TreeMap<>();
        final AccessControlList acl = new AccessControlList(
            objPath,
            aclBackingMap,
            objProtAclDriver,
            transObjFactory,
            transMgrProvider
        );

        final ObjectProtection objProt = new ObjectProtection(
            impersonatedAccCtx,
            objPath,
            acl,
            this,
            transObjFactory,
            transMgrProvider
        );

        return new Pair<>(
            objProt,
            new SecObjProtInitObj(objPath, aclBackingMap)
        );
    }

    @Override
    protected String getId(ObjectProtection dataRef) throws AccessDeniedException
    {
        return "Object protection: " + dataRef.getObjectProtectionPath();
    }
}
