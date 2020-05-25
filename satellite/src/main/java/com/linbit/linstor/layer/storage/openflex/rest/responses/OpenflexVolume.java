package com.linbit.linstor.layer.storage.openflex.rest.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenflexVolume
{
    public String Self;
    public String ID;
    public OpenflexStatus Status;
    public long Capacity;
    public String Pools;
    public int Connections;
    public String CreationDate;
    public String LastModified;
    public String Name;
    public String NQN;
    public String PoolID;
    public OpenflexRemoteConnections RemoteConnections;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OpenflexRemoteConnections
    {
        public Member[] Members;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Member
    {
        public String RemoteConnection;
    }

    /*
     {
      "Self": "http://10.43.7.185:80/Storage/Devices/000af795789d/Volumes/1c195ffa-48a9-45ec-9052-2fdceac2656e/",
      "ID": "1c195ffa-48a9-45ec-9052-2fdceac2656e",
      "Status": {
        "State": {
          "ID": 16,
          "Name": "In service"
        },
        "Health": [
          {
            "ID": 5,
            "Name": "OK"
          }
        ]
      },
      "Capacity": 1073741824,
      "UUID": "1c195ffa-48a9-45ec-9052-2fdceac2656e",
      "Pools": "http://10.43.7.185:80/Storage/Devices/000af795789d/Pools/?volumeid=1c195ffa-48a9-45ec-9052-2fdceac2656e",
      "AllowAnyHost": true,
      "ActiveQPairs": 0,
      "Connections": 0,
      "CreateDate": "Thu Jan 23 09:17:30 2020",
      "Description": "",
      "Extendable": true,
      "LastModified": "Thu Jan 23 09:17:30 2020",
      "Manufacturer": "WDC",
      "Model": "OpenFlex F3000",
      "Name": "test",
      "NQN": "nqn.1992-05.com.wdc.f3000-95789d:test",
      "PoolID": "0",
      "RemoteConnections": {
        "Members": [
          {
            "RemoteConnection": "192.168.166.1:43375"
          }
        ]
      },
      "SerialNumber": "SN000AF795789C00001B"
    }
     */
}
