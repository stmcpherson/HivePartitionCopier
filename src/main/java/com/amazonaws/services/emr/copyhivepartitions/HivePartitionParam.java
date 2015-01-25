package com.amazonaws.services.emr.copyhivepartitions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class HivePartitionParam {
  
  public int PART_ID = 0; 
  public String PARAM_KEY;
  public String PARAM_VALUE; 
  
  
  public HivePartitionParam(ResultSet resultSet, HashMap<Integer, Integer> partmap) throws SQLException{
    PART_ID = partmap.get(resultSet.getInt("PART_ID"));
    PARAM_KEY = resultSet.getString("PARAM_KEY");
    PARAM_VALUE = resultSet.getString("PARAM_VALUE");
    
  }
  public static String ToSQLInsertPrefix(){
    String sqlout = "INSERT INTO PARTITION_PARAMS " +
     "(PART_ID, PARAM_KEY, PARAM_VALUE) "
        + "VALUES "; 
       return sqlout; 
  }

  public String ToSQLInsert(boolean isFirst){
    String sqlout = "";
    if(!isFirst){
      sqlout+= ",";
    }
    sqlout += "((" +PART_ID+ ", '" +PARAM_KEY+ "', '"+PARAM_VALUE+"')"; 
    return sqlout; 
  }

}
