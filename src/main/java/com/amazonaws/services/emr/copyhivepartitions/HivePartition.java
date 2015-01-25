package com.amazonaws.services.emr.copyhivepartitions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class HivePartition {
  

  public int PART_ID = 0; 
  public String CREATE_TIME;
  public String LAST_ACCESS_TIME;
  public String PART_NAME; 
  public int SD_ID = 0; 
  public int TBL_ID = 0;
  public int NEW_ID; 
  
  public HivePartition(ResultSet resultSet, int new_id, int new_tbl, HashMap<Integer, Integer> map) throws SQLException{
    PART_ID = resultSet.getInt("PART_ID");
    CREATE_TIME = resultSet.getString("CREATE_TIME");
    LAST_ACCESS_TIME = resultSet.getString("LAST_ACCESS_TIME");
    PART_NAME = resultSet.getString("PART_NAME");
    SD_ID = map.get(resultSet.getInt("SD_ID"));
    TBL_ID = new_tbl;
    NEW_ID = new_id;
    
  }
 public HivePartition(String serdestring) {
    
    String[] split = serdestring.split("\\t");
    PART_ID = Integer.parseInt(split[0]);
    NEW_ID = Integer.parseInt(split[1]);
  }
  
 public static String ToSQLInsertPrefix(){
   String sqlout = "INSERT INTO PARTITIONS " +
       "(PART_ID, CREATE_TIME, LAST_ACCESS_TIME, PART_NAME, SD_ID, TBL_ID, LINK_TARGET_ID) "
          + "VALUES "; 
      return sqlout; 
 }
 

  public String ToSQLInsert(boolean isFirst){
    String sqlout = "";
    if(!isFirst){
      sqlout+= ",";
    }
    sqlout += "(" +NEW_ID+ ", '" +CREATE_TIME+ "', '"+LAST_ACCESS_TIME+"','"+PART_NAME+"', "+SD_ID+", "+TBL_ID+", null)"; 
    return sqlout; 
  }
 

}
