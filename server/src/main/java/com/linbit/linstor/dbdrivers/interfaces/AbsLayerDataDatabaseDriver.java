package com.linbit.linstor.dbdrivers.interfaces;

public interface AbsLayerDataDatabaseDriver<LAYER_DATA> extends GenericDatabaseDriver<LAYER_DATA>
{
    LayerResourceIdDatabaseDriver getIdDriver();
}
