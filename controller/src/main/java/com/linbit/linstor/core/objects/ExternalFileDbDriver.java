package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.objects.ExternalFile.InitMaps;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.ByteUtils;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.CONTENT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.CONTENT_CHECKSUM;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.PATH;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public final class ExternalFileDbDriver extends AbsProtectedDatabaseDriver<ExternalFile, ExternalFile.InitMaps, Void>
    implements ExternalFileCtrlDatabaseDriver
{
    protected final PropsContainerFactory propsContainerFactory;
    protected final TransactionObjectFactory transObjFactory;
    protected final Provider<? extends TransactionMgr> transMgrProvider;

    protected final SingleColumnDatabaseDriver<ExternalFile, byte[]> contentDriver;
    protected final SingleColumnDatabaseDriver<ExternalFile, byte[]> contentChecksumDriver;
    protected final StateFlagsPersistence<ExternalFile> flagsDriver;
    protected final AccessContext dbCtx;

    @Inject
    public ExternalFileDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.FILES, dbEngine, objProtFactoryRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, extFile -> extFile.getUuid().toString());
        setColumnSetter(PATH, extFile -> extFile.getName().extFileName);
        setColumnSetter(FLAGS, extFile -> extFile.getFlags().getFlagsBits(dbCtxRef));
        setColumnSetter(CONTENT_CHECKSUM, extFile -> extFile.getContentCheckSumHex(dbCtxRef));

        switch (getDbType())
        {
            case ETCD:
                setColumnSetter(CONTENT, extFile -> Base64.encode(extFile.getContent(dbCtxRef)));
                contentDriver = generateSingleColumnDriver(
                    CONTENT,
                    extFile -> new String(extFile.getContent(dbCtxRef)),
                    Base64::encode
                );
                break;
            case SQL: // fall-through
            case K8S_CRD:
                setColumnSetter(CONTENT, extFile -> extFile.getContent(dbCtxRef));
                contentDriver = generateSingleColumnDriver(
                    CONTENT,
                    extFile -> new String(extFile.getContent(dbCtxRef)),
                    Function.identity()
                );
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        flagsDriver = generateFlagDriver(FLAGS, ExternalFile.Flags.class);
        contentChecksumDriver = generateSingleColumnDriver(
            CONTENT_CHECKSUM,
            extFile -> ByteUtils.bytesToHex(extFile.getContentCheckSum(dbCtxRef)),
            byteArr -> ByteUtils.bytesToHex(byteArr)
        );
    }

    @Override
    public StateFlagsPersistence<ExternalFile> getStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentDriver()
    {
        return contentDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentCheckSumDriver()
    {
        return contentChecksumDriver;
    }

    @Override
    protected String getId(ExternalFile dataRef) throws AccessDeniedException
    {
        return "External file(" + dataRef.getName().extFileName + ")";
    }

    @Override
    protected Pair<ExternalFile, InitMaps> load(RawParameters raw, Void ignored)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final ExternalFileName extFileName = raw.build(PATH, ExternalFileName::new);
        final byte[] content;
        final byte[] contentCheckSum;
        try
        {
            contentCheckSum = ByteUtils.hexToBytes(raw.get(CONTENT_CHECKSUM));
        }
        catch (IllegalArgumentException illArgExc)
        {
            throw new DatabaseException(
                "Database column " + CONTENT_CHECKSUM.getName() + " contains invalid data",
                illArgExc
            );
        }
        final long initFlags;

        switch (getDbType())
        {
            case ETCD:
                content = Base64.decode(raw.get(CONTENT));
                initFlags = Long.parseLong(raw.get(FLAGS));
                setColumnSetter(CONTENT, extFile -> Base64.encode(extFile.getContent(dbCtx)));
                break;
            case SQL: // fall-through
            case K8S_CRD:
                content = raw.get(CONTENT);
                initFlags = raw.get(FLAGS);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        return new Pair<>(
            new ExternalFile(
                raw.build(UUID, java.util.UUID::fromString),
                getObjectProtection(ObjectProtection.buildPath(extFileName)),
                extFileName,
                initFlags,
                content,
                contentCheckSum,
                this,
                transObjFactory,
                transMgrProvider
            ),
            new InitMapsImpl()
        );
    }

    private class InitMapsImpl implements ExternalFile.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
