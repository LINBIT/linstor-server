package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.VlmGrpPojo;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class VolumeGroup extends BaseTransactionObject
    implements ProtectedObject, DbgInstanceUuid, Comparable<VolumeGroup>
{
    private final UUID objId;

    private final transient UUID dbgInstanceId;

    private final ResourceGroup rscGrp;

    private final VolumeNumber vlmNr;

    private final Props vlmGrpProps;

    private final StateFlags<VolumeGroup.Flags> flags;

    private final VolumeGroupDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeGroup, Boolean> deleted;

    public VolumeGroup(
        UUID uuidRef,
        ResourceGroup rscGrpRef,
        VolumeNumber vlmNrRef,
        long initFlags,
        VolumeGroupDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);

        objId = uuidRef;
        vlmNr = vlmNrRef;
        dbgInstanceId = UUID.randomUUID();
        rscGrp = rscGrpRef;

        dbDriver = dbDriverRef;

        flags = transObjFactoryRef.createStateFlagsImpl(
            rscGrpRef.getObjProt(),
            this,
            VolumeGroup.Flags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        vlmGrpProps = propsContainerFactoryRef.getInstance(
            PropsContainer.buildPath(rscGrp.getName(), vlmNr)
        );
        deleted = transObjFactoryRef.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            rscGrp,
            flags,
            vlmGrpProps,
            deleted
        );
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return rscGrp.getObjProt();
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public ResourceGroup getResourceGroup()
    {
        return rscGrp;
    }

    public VolumeNumber getVolumeNumber()
    {
        return vlmNr;
    }

    public Props getProps(AccessContext accCtxRef) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtxRef, rscGrp.getObjProt(), vlmGrpProps);
    }

    public StateFlags<VolumeGroup.Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public void delete(AccessContext accCtxRef) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            rscGrp.getObjProt().requireAccess(accCtxRef, AccessType.USE);

            rscGrp.deleteVolumeGroup(accCtxRef, vlmNr);
            vlmGrpProps.delete();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume group");
        }
    }

    @Override
    public int compareTo(VolumeGroup other)
    {
        checkDeleted();
        int result = rscGrp.getName().compareTo(other.getResourceGroup().getName());
        if (result == 0)
        {
            result = vlmNr.compareTo(other.getVolumeNumber());
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(rscGrp, vlmNr);
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
        else if (obj instanceof VolumeGroup)
        {
            VolumeGroup other = (VolumeGroup) obj;
            other.checkDeleted();
            ret = Objects.equals(rscGrp, other.rscGrp) && Objects.equals(vlmNr, other.vlmNr);
        }
        return ret;
    }

    public VolumeGroupApi getApiData(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new VlmGrpPojo(
            objId,
            vlmNr.value,
            Collections.unmodifiableMap(vlmGrpProps.map()),
            flags.getFlagsBits(accCtxRef)
        );
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        GROSS_SIZE(1L);

        public final long flagValue;

        Flags(long value)
        {
            flagValue = value;
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

        public static Flags[] restoreFlags(long vlmDfnFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((vlmDfnFlags & flag.flagValue) == flag.flagValue)
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
}
