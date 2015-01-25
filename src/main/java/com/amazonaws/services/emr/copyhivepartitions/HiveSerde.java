package com.amazonaws.services.emr.copyhivepartitions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class HiveSerde {

  public ArrayList<HiveSerdeParam> SerdeParams;
  public ArrayList<HiveSD> SDs;

  public int SERDE_ID = 0;
  public String NAME;
  public String SLIB;
  public int NEW_ID;

  public HiveSerde(ResultSet resultSet, int new_id) throws SQLException {
    SERDE_ID = resultSet.getInt("SERDE_ID");
    NAME = resultSet.getString("NAME");
    SLIB = resultSet.getString("SLIB");
    NEW_ID = new_id;

  }

  public HiveSerde(String serdestring) {

    String[] split = serdestring.split("\\t");
    SERDE_ID = Integer.parseInt(split[0]);
    NEW_ID = Integer.parseInt(split[1]);
  }

  public String ToTempTSV() {
    return SERDE_ID + "\t" + NEW_ID;
  }

  public static String ToSQLInsertPrefix() {
    String sqlout = "INSERT INTO SERDES " + "(SERDE_ID, NAME, SLIB) " + "VALUES ";
    return sqlout;
  }

  public String ToSQLInsert(boolean isFirst) {
    String sqlout = "";
    if (!isFirst) {
      sqlout += ",";
    }
    sqlout += "( " + NEW_ID + ", " + NAME + ", '" + SLIB + "')";
    return sqlout;
  }

}
