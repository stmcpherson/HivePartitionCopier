package com.amazonaws.services.emr.copyhivepartitions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class HivePartitionKeyVal {
 
  public int PART_ID = 0; 
  public String PART_KEY_VAL;
  public int INTEGER_IDX = 0; 
  
  
  public HivePartitionKeyVal(ResultSet resultSet, HashMap<Integer, Integer> partmap) throws SQLException{
    PART_ID = partmap.get(resultSet.getInt("PART_ID"));
    PART_KEY_VAL = resultSet.getString("PART_KEY_VAL");
    INTEGER_IDX = resultSet.getInt("INTEGER_IDX");
    
  }
 
  public static String ToSQLInsertPrefix(){
    String sqlout = "INSERT INTO PARTITION_KEY_VALS " +
        "(PART_ID, PART_KEY_VAL, INTEGER_IDX) "
           + "VALUES "; 
       return sqlout; 
  }
  
  public String ToSQLInsert(boolean isFirst){
    String sqlout = "";
    if(!isFirst){
      sqlout+= ",";
    }
    sqlout += "(" +PART_ID+ ", '" +PART_KEY_VAL+ "', "+INTEGER_IDX+")"; 
    return sqlout; 
  }

}
