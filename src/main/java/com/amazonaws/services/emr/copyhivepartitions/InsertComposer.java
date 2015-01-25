package com.amazonaws.services.emr.copyhivepartitions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

public class InsertComposer {
  
  
  public static void Run(Properties props){
    
    DBQueryRunner qr = null;
    try {
      qr = new DBQueryRunner(props);
    } catch (IOException e) {
      e.printStackTrace();
    } 
    try {
      System.out.println("Testing Database Connection...");
      System.out.println("");
      System.out.println("");
      if (qr.TestConnection())
      {
        System.out.println("Successfully connected to the database");
        System.out.println("");
        System.out.println("");
      }
    } catch (ClassNotFoundException e) {
      System.out.println("FAILED TO CONNECT TO THE DATABASE. Check your settings.");
      System.out.println("");
      System.out.println("");
      return; 
    } catch (SQLException e) {
      System.out.println("FAILED TO CONNECT TO THE DATABASE. Check your settings.");
      System.out.println("");
      System.out.println("");
      return; 
    }
    try {
      System.out.println("Fetching Serdes...");
      HashMap<Integer, Integer> serdesmap = qr.GetSerdes();
      System.out.println("Found "+serdesmap.size()+ " Serdes in the database");
      System.out.println("");
      System.out.println("");
      System.out.println("Fetching SDS objects...");
      HashMap<Integer, Integer> sdssmap = qr.GetSDs(serdesmap);
      System.out.println("Found "+sdssmap.size()+ " SDS objects in the database");
      System.out.println("");
      System.out.println("");
      System.out.println("Fetching Partition objects...");
      HashMap<Integer, Integer> partmap =  qr.GetPartitions(sdssmap);
      System.out.println("Found "+partmap.size()+ " Partition objects in the database");
      System.out.println("");
      System.out.println("");
      System.out.println("Fetching SerdeParameters objects...");
      qr.GetSerdeParms(serdesmap);
      System.out.println("Comptleted loading SerdeParameters");
      System.out.println("");
      System.out.println("");
      System.out.println("Fetching PartitionParams objects...");
      qr.GetPartitionParams(partmap);
      System.out.println("Comptleted loading PartitionKeyParams");
      System.out.println("");
      System.out.println("");
      System.out.println("Fetching PartitionKeyValue objects...");
      qr.GetPartitionKeyValues(partmap);
      System.out.println("Comptleted loading PartitionKeyValue");
      
      System.out.println("");
      System.out.println("");
      
      System.out.println("Output File has been written to [" + props.getProperty("output.file")+"]");
      System.out.println("");
      
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    
  }

}
