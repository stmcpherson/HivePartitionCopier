package com.amazonaws.services.emr.copyhivepartitions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class HiveSD {

  public int SD_ID = 0;
  public int CD_ID = 0;
  public String INPUT_FORMAT;
  public int IS_COMPRESSED = 0;
  public int IS_STOREDASSUBDIRECTORIES = 0;
  public String LOCATION;
  public int NUM_BUCKETS = 0;
  public String OUTPUT_FORMAT;
  public int SERDE_ID = 0;
  public int NEW_ID;

  public HiveSD(ResultSet resultSet, int new_tbl, int new_id, HashMap<Integer, Integer> map) throws SQLException {
    SD_ID = resultSet.getInt("SD_ID");

    INPUT_FORMAT = resultSet.getString("INPUT_FORMAT");
    IS_COMPRESSED = resultSet.getInt("IS_COMPRESSED");
    IS_STOREDASSUBDIRECTORIES = resultSet.getInt("IS_STOREDASSUBDIRECTORIES");
    LOCATION = resultSet.getString("LOCATION");
    NUM_BUCKETS = resultSet.getInt("NUM_BUCKETS");
    OUTPUT_FORMAT = resultSet.getString("OUTPUT_FORMAT");
    SERDE_ID = map.get(resultSet.getInt("SERDE_ID"));
    NEW_ID = new_id;
    CD_ID = new_tbl;
  }

  public HiveSD(String sdstring) {

    String[] split = sdstring.split("\\t");
    SD_ID = Integer.parseInt(split[0]);
    NEW_ID = Integer.parseInt(split[1]);
  }

  public static String ToSQLInsertPrefix() {
    String sqlout = "INSERT INTO SDS "
        + "(SD_ID, CD_ID, INPUT_FORMAT, IS_COMPRESSED, IS_STOREDASSUBDIRECTORIES, LOCATION, NUM_BUCKETS, OUTPUT_FORMAT, SERDE_ID) "
        + "VALUES ";
    return sqlout;
  }

  public String ToSQLInsert(boolean isFirst) {
    String sqlout = "";
    if (!isFirst) {
      sqlout += ",";
    }
    sqlout += "(" + NEW_ID + ", " + CD_ID + ", '" + INPUT_FORMAT + "' , " + IS_COMPRESSED + ", "
        + IS_STOREDASSUBDIRECTORIES + ", '" + LOCATION + "', " + NUM_BUCKETS + ", '" + OUTPUT_FORMAT + "', " + SERDE_ID
        + ")";
    return sqlout;
  }

}
