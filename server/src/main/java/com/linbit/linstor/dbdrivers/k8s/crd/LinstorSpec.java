package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;

import java.io.Serializable;
import java.util.Map;

public interface LinstorSpec extends Serializable
{
    Map<String, Object> asRawParameters();

    Object getByColumn(Column clm);

    String getLinstorKey();

    DatabaseTable getDatabaseTable();
}
