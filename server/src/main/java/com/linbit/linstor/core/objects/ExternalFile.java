package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.ByteUtils;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class ExternalFile extends AbsCoreObj<ExternalFile> implements ProtectedObject
{
    // File name (not the path, more like a short descriptive name for linstor)
    private final ExternalFileName fileName;

    // State flags
    private final StateFlags<Flags> flags;

    private final TransactionSimpleObject<ExternalFile, byte[]> content;
    private final TransactionSimpleObject<ExternalFile, byte[]> contentCheckSum;
    private final TransactionSimpleObject<ExternalFile, Boolean> alreadyWritten; // stlt only

    private final ObjectProtection objProt;
    private final ExternalFileDatabaseDriver dbDriver;


    ExternalFile(
        UUID uuidRef,
        ObjectProtection objProtRef,
        ExternalFileName extFileNameRef,
        long initFlagsRef,
        byte[] contentRef,
        byte[] contentCheckSumRef,
        ExternalFileDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(uuidRef, transObjFactory, transMgrProviderRef);
        ErrorCheck.ctorNotNull(ExternalFile.class, ExternalFileName.class, extFileNameRef);
        ErrorCheck.ctorNotNull(ExternalFile.class, byte[].class, contentRef);

        objProt = objProtRef;
        fileName = extFileNameRef;
        dbDriver = dbDriverRef;

        content = transObjFactory.createTransactionSimpleObject(this, contentRef, dbDriver.getContentDriver());
        contentCheckSum = transObjFactory.createTransactionSimpleObject(
            this,
            contentCheckSumRef,
            dbDriver.getContentCheckSumDriver()
        );
        alreadyWritten = transObjFactory.createTransactionSimpleObject(
            this,
            false,
            null
        );

        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            Flags.class,
            dbDriver.getStateFlagPersistence(),
            initFlagsRef
        );

        transObjs = Arrays.asList(content, flags);
    }

    @Override
    public int compareTo(ExternalFile otherFile)
    {
        return fileName.compareTo(otherFile.getName());
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(fileName);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof ExternalFile)
        {
            ExternalFile other = (ExternalFile) obj;
            other.checkDeleted();
            ret = Objects.equals(fileName, other.fileName);
        }
        return ret;
    }

    public ExternalFileName getName()
    {
        checkDeleted();
        return fileName;
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public byte[] getContentCheckSum(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return contentCheckSum.get();
    }

    public String getContentCheckSumHex(AccessContext accCtx) throws AccessDeniedException
    {
        return ByteUtils.bytesToHex(getContentCheckSum(accCtx));
    }

    /*
     * used by satellite only -> no access check
     */
    public void setAlreadyWritten(boolean alreadyWrittenRef) throws DatabaseException
    {
        checkDeleted();
        alreadyWritten.set(alreadyWrittenRef);
    }

    /*
     * used by satellite only -> no access check
     */
    public boolean alreadyWritten()
    {
        checkDeleted();
        return alreadyWritten.get();
    }

    public byte[] getContent(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return content.get();
    }

    public void setContent(AccessContext accCtx, byte[] contentRef) throws DatabaseException, AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        byte[] checksum = ByteUtils.checksumSha256(contentRef);
        content.set(contentRef);
        contentCheckSum.set(checksum);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    public ExternalFilePojo getApiData(
        AccessContext accCtx,
        @Nullable Long fullSyncId,
        @Nullable Long updateId
    )
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return new ExternalFilePojo(
            objId,
            fileName.extFileName,
            flags.getFlagsBits(accCtx),
            content.get(),
            contentCheckSum.get(),
            fullSyncId,
            updateId
        );
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            objProt.delete(accCtx);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public interface InitMaps
    {
        // empty for now
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L);

        public long flagValue;

        Flags(long valueRef)
        {
            flagValue = valueRef;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static Flags[] valuesOfIgnoreCase(String string)
        {
            Flags[] flags;
            if (string == null)
            {
                flags = new Flags[0];
            }
            else
            {
                String[] split = string.split(",");
                flags = new Flags[split.length];

                for (int idx = 0; idx < split.length; idx++)
                {
                    flags[idx] = Flags.valueOf(split[idx].toUpperCase().trim());
                }
            }
            return flags;
        }

        public static Flags[] restoreFlags(long extFileFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((extFileFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new Flags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(Flags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(Flags.class, listFlags);
        }
    }

    @Override
    protected String toStringImpl()
    {
        return "External File '" + fileName + "'";
    }
}
