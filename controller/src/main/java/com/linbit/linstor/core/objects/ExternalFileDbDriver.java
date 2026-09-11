package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.objects.ExternalFile.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.ByteUtils;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.ALT_SUFFIXES;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.CONTENT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.CONTENT_CHECKSUM;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.PATH;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Files.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.function.Function;

@Singleton
public final class ExternalFileDbDriver extends AbsDatabaseDriver<ExternalFile, ExternalFile.InitMaps, Void>
    implements ExternalFileCtrlDatabaseDriver
{
    final PropsContainerFactory propsContainerFactory;
    final TransactionObjectFactory transObjFactory;
    final Provider<? extends TransactionMgr> transMgrProvider;

    final SingleColumnDatabaseDriver<ExternalFile, byte[]> contentDriver;
    final SingleColumnDatabaseDriver<ExternalFile, byte[]> contentChecksumDriver;
    final CollectionDatabaseDriver<ExternalFile, String> altSuffixesDriver;
    final StateFlagsPersistence<ExternalFile> flagsDriver;

    @Inject
    public ExternalFileDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.FILES, dbEngine);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, extFile -> extFile.getUuid().toString());
        setColumnSetter(PATH, extFile -> extFile.getName().extFileName);
        setColumnSetter(FLAGS, extFile -> extFile.getFlags().getFlagsBits());
        setColumnSetter(CONTENT_CHECKSUM, extFile -> extFile.getContentCheckSumHex());

        setColumnSetter(CONTENT, extFile -> extFile.getContent());
        contentDriver = generateSingleColumnDriver(
            CONTENT,
            extFile -> new String(extFile.getContent(), StandardCharsets.UTF_8),
            Function.identity()
        );

        setColumnSetter(ALT_SUFFIXES, extFile -> toBlob(extFile.getAltSuffixes()));
        altSuffixesDriver = generateCollectionToJsonStringArrayDriver(ALT_SUFFIXES);

        flagsDriver = generateFlagDriver(FLAGS, ExternalFile.Flags.class);
        contentChecksumDriver = generateSingleColumnDriver(
            CONTENT_CHECKSUM,
            extFile -> ByteUtils.bytesToHex(extFile.getContentCheckSum()),
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
    public CollectionDatabaseDriver<ExternalFile, String> getAltSuffixesDriver()
    {
        return altSuffixesDriver;
    }

    @Override
    protected String getId(ExternalFile dataRef)
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

        content = raw.get(CONTENT);
        initFlags = raw.get(FLAGS);

        return new Pair<>(
            new ExternalFile(
                raw.build(UUID, java.util.UUID::fromString),
                extFileName,
                initFlags,
                content,
                contentCheckSum,
                new ArrayList<>(raw.getAsStringListNonNull(ALT_SUFFIXES)),
                this,
                transObjFactory,
                transMgrProvider
            ),
            new InitMapsImpl()
        );
    }

    private static class InitMapsImpl implements ExternalFile.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
