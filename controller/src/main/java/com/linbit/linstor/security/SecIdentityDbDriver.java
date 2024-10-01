package com.linbit.linstor.security;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver.SecIdentityDbObj;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.IDENTITY_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.ID_ENABLED;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.ID_LOCKED;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.PASS_HASH;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.PASS_SALT;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class SecIdentityDbDriver extends AbsDatabaseDriver<SecIdentityDbObj, Void, Void>
    implements SecIdentityCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> passHashDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> passSaltDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> idEnabledDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> idLockedDriver;

    @Inject
    public SecIdentityDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_IDENTITIES, dbEngineRef);
        dbCtx = dbCtxRef;

        setColumnSetter(IDENTITY_NAME, id -> id.getIdentity().name.value);
        setColumnSetter(IDENTITY_DSP_NAME, id -> id.getIdentity().name.displayValue);


        switch (getDbType()) {
            case SQL: // fall-through
            case K8S_CRD:
                setColumnSetter(PASS_HASH, SecIdentityDbObj::getPassHash);
                setColumnSetter(PASS_SALT, SecIdentityDbObj::getPassSalt);
                setColumnSetter(ID_ENABLED, SecIdentityDbObj::isEnabled);
                setColumnSetter(ID_LOCKED, SecIdentityDbObj::isLocked);

                passHashDriver = generateSingleColumnDriver(
                    PASS_HASH,
                    ignored -> MSG_DO_NOT_LOG,
                    Function.identity()
                );
                passSaltDriver = generateSingleColumnDriver(
                    PASS_SALT,
                    ignored -> MSG_DO_NOT_LOG,
                    Function.identity()
                );

                idEnabledDriver = generateSingleColumnDriver(
                    ID_ENABLED,
                    this::getId,
                    Function.identity()
                );
                idLockedDriver = generateSingleColumnDriver(
                    ID_LOCKED,
                    this::getId,
                    Function.identity()
                );
                break;
            case ETCD:
                setColumnSetter(PASS_HASH, id -> Base64.encode(id.getPassHash()));
                setColumnSetter(PASS_SALT, id -> Base64.encode(id.getPassSalt()));
                setColumnSetter(ID_ENABLED, id -> Boolean.toString(id.isEnabled()));
                setColumnSetter(ID_LOCKED, id -> Boolean.toString(id.isLocked()));

                passHashDriver = generateSingleColumnDriver(
                    PASS_HASH,
                    ignored -> MSG_DO_NOT_LOG,
                    Base64::encode
                );
                passSaltDriver = generateSingleColumnDriver(
                    PASS_SALT,
                    ignored -> MSG_DO_NOT_LOG,
                    Base64::encode
                );

                idEnabledDriver = generateSingleColumnDriver(
                    ID_ENABLED,
                    this::getId,
                    enabled -> Boolean.toString(enabled)
                );
                idLockedDriver = generateSingleColumnDriver(
                    ID_LOCKED,
                    this::getId,
                    locked -> Boolean.toString(locked)
                );
                break;
            default:
                throw new ImplementationError("Unexpected Db type: " + getDbType());
        }
    }
    @Override
    public SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> getPassHashDriver()
    {
        return passHashDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> getPassSaltDriver()
    {
        return passSaltDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> getIdEnabledDriver()
    {
        return idEnabledDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> getIdLockedDriver()
    {
        return idLockedDriver;
    }

    @Override
    protected Pair<SecIdentityDbObj, Void> load(RawParameters rawRef, Void parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        IdentityName idName = rawRef.build(IDENTITY_DSP_NAME, IdentityName::new);
        byte[] passHash;
        byte[] passSalt;
        boolean enabled = rawRef.getParsed(ID_ENABLED);
        boolean locked = rawRef.getParsed(ID_LOCKED);

        switch (getDbType())
        {
            case SQL:
                // since the SqlEngine stores CHAR as String, we also need to load them as String
                passHash = rawRef.<String, byte[], AccessDeniedException>build(PASS_HASH, str -> str.getBytes());
                passSalt = rawRef.<String, byte[], AccessDeniedException>build(PASS_SALT, str -> str.getBytes());
                break;
            case K8S_CRD:
                passHash = rawRef.get(PASS_HASH);
                passSalt = rawRef.get(PASS_SALT);
                break;
            case ETCD:
                passHash = rawRef.buildParsed(PASS_HASH, Base64::decode);
                passSalt = rawRef.buildParsed(PASS_SALT, Base64::decode);
                break;
            default:
                throw new ImplementationError("Unexpected Db type: " + getDbType());
        }

        return new Pair<>(
            new SecIdentityDbObj(Identity.create(dbCtx, idName), passHash, passSalt, enabled, locked),
            null
        );
    }

    @Override
    protected String getId(SecIdentityDbObj dataRef) throws AccessDeniedException
    {
        return "Identity: " + dataRef.getIdentity().name.displayValue;
    }
}
