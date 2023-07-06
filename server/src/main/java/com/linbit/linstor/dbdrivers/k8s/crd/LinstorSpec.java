package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.linstor.dbdrivers.DatabaseTable;

import java.io.Serializable;
import java.util.Map;

public interface LinstorSpec<CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> extends Serializable
{
    CRD getCrd();

    Map<String, Object> asRawParameters();

    default Object getByColumn(DatabaseTable.Column clm)
    {
        return getByColumn(clm.getName());
    }

    Object getByColumn(String clmNameStr);

    String getLinstorKey();

    DatabaseTable getDatabaseTable();
}
