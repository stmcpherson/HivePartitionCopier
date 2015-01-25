package com.amazonaws.services.emr.copyhivepartitions;

import java.util.HashMap;
import java.util.Properties;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBQueryRunner {

  static String password = "";
  static String url = "";
  static String username = "";
  static String driverClassName = "";

  static String sourceTableName;
  static String targetTableName;

  static String outputfile;

  HiveRowIndexes rowIndexes;

  public DBQueryRunner(Properties props) throws IOException, ClassNotFoundException, SQLException {
    if (props == null) {
      props = new ConfigReader().getPropValuesFromFile();
    }
    password = props.getProperty("database.password");
    url = props.getProperty("database.url");
    username = props.getProperty("database.username");
    driverClassName = props.getProperty("database.driverClassName");
    sourceTableName = props.getProperty("sourceTableName");
    targetTableName = props.getProperty("targetTableName");
    outputfile = props.getProperty("output.file");
    rowIndexes = this.GetTableIndexes();
  }

  public boolean TestConnection() throws ClassNotFoundException, SQLException {
    String query = "show tables;";

    ResultSet resultSet = null;
    boolean connected = false;

    Connection connection = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      while (resultSet.next()) {
        connected = true;
        break;
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
    return connected;
  }

  public int GetTableId(String tableName) throws SQLException, ClassNotFoundException {

    String query = "Select TBL_ID from TBLS where TBL_NAME ='" + tableName + "' LIMIT 1;";

    ResultSet resultSet = null;
    int tbl_id = 0;
    Connection connection = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      while (resultSet.next()) {
        tbl_id = resultSet.getInt("TBL_ID");
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }

    return tbl_id;
  }

  public int GetPartitionIndexId() throws ClassNotFoundException, SQLException {
    String query = "select PART_ID from PARTITIONS order by PART_ID desc limit 1";

    ResultSet resultSet = null;
    int part_id = 0;
    Connection connection = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      while (resultSet.next()) {
        part_id = resultSet.getInt(1);
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }

    return part_id;
  }

  public int GetSerdeIndexId() throws ClassNotFoundException, SQLException {
    String query = "select SERDE_ID from SERDES order by SERDE_ID desc limit 1";

    ResultSet resultSet = null;
    int serde_id = 0;
    Connection connection = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      while (resultSet.next()) {
        serde_id = resultSet.getInt(1);
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
    return serde_id;
  }

  public int GetSDIndexId() throws ClassNotFoundException, SQLException {
    String query = "select SD_ID from SDS order by SD_ID desc limit 1";
    ResultSet resultSet = null;
    int sd_id = 0;
    Connection connection = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      while (resultSet.next()) {
        sd_id = resultSet.getInt(1);
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
    return sd_id;
  }

  public HashMap<Integer, Integer> GetPartitions(HashMap<Integer, Integer> sdsmap) throws IOException,
      ClassNotFoundException, SQLException {

    HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
    String query = "Select * from PARTITIONS where TBL_ID = " + rowIndexes.SOURCE_TABLE_ID + ";";
    ResultSet resultSet = null;
    Connection connection = null;
    PrintWriter writer = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      int i = rowIndexes.PART_ID;
      int new_tbl = rowIndexes.TARGET_TABLE_ID;
      int ix = 0;
      writer = new PrintWriter(new BufferedWriter(new FileWriter(outputfile, true)));
      writer.println(HivePartition.ToSQLInsertPrefix());
      while (resultSet.next()) {
        ix++;
        i++;
        HivePartition item = new HivePartition(resultSet, i, new_tbl, sdsmap);

        map.put(item.PART_ID, i);
        boolean isFirst = (ix == 1);
        writer.println(item.ToSQLInsert(isFirst));
      }
      i++;
      writer.println(";");
      writer.println("UPDATE SEQUENCE_TABLE SET NEXT_VAL='" + i
          + "' WHERE SEQUENCE_NAME='org.apache.hadoop.hive.metastore.model.MPartition';");
    } finally {
      if (connection != null) {
        connection.close();
      }
      if (writer != null) {
        writer.close();
      }
    }
    return map;
  }

  public void GetPartitionKeyValues(HashMap<Integer, Integer> partmap) throws IOException, ClassNotFoundException,
      SQLException {

    String query = "Select PARTITION_KEY_VALS.* from PARTITION_KEY_VALS " + "join (TBLS, PARTITIONS) "
        + "ON (PARTITION_KEY_VALS.PART_ID = PARTITIONS.PART_ID AND PARTITIONS.TBL_ID = TBLS.TBL_ID) "
        + "where TBLS.TBL_ID =" + rowIndexes.SOURCE_TABLE_ID + ";";
    ResultSet resultSet = null;
    Connection connection = null;
    PrintWriter writer = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      writer = new PrintWriter(new BufferedWriter(new FileWriter(outputfile, true)));
      writer.println(HivePartitionKeyVal.ToSQLInsertPrefix());
      int ix = 0;
      while (resultSet.next()) {
        ix++;
        HivePartitionKeyVal item = new HivePartitionKeyVal(resultSet, partmap);
        boolean isFirst = (ix == 1);
        writer.println(item.ToSQLInsert(isFirst));
      }
      writer.println(";");
    } finally {
      if (connection != null) {
        connection.close();
      }
      if (writer != null) {
        writer.close();
      }
    }
    return;
  }

  public void GetPartitionParams(HashMap<Integer, Integer> partmap) throws IOException, ClassNotFoundException,
      SQLException {

    String query = "Select PARTITION_PARAMS.* from PARTITION_PARAMS " + "join (TBLS, PARTITIONS) "
        + "ON (PARTITION_PARAMS.PART_ID = PARTITIONS.PART_ID AND PARTITIONS.TBL_ID = TBLS.TBL_ID) "
        + "where TBLS.TBL_ID =" + rowIndexes.SOURCE_TABLE_ID + "; ";

    ResultSet resultSet = null;
    Connection connection = null;
    PrintWriter writer = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      writer = new PrintWriter(new BufferedWriter(new FileWriter(outputfile, true)));
      writer.println(HivePartitionParam.ToSQLInsertPrefix());
      int ix = 0;
      while (resultSet.next()) {
        ix++;
        HivePartitionParam item = new HivePartitionParam(resultSet, partmap);
        boolean isFirst = (ix == 1);
        writer.println(item.ToSQLInsert(isFirst));
      }
      writer.println(";");
    } finally {
      if (connection != null) {
        connection.close();
      }
      if (writer != null) {
        writer.close();
      }
    }
    return;
  }

  public HashMap<Integer, Integer> GetSerdes() throws IOException, SQLException, ClassNotFoundException {
    HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

    String query = "Select SERDES.* from SERDES " + "join (SDS, PARTITIONS, TBLS) "
        + "ON (SERDES.SERDE_ID = SDS.SERDE_ID AND SDS.SD_ID = PARTITIONS.SD_ID AND PARTITIONS.TBL_ID = TBLS.TBL_ID) "
        + "where TBLS.TBL_ID =" + rowIndexes.SOURCE_TABLE_ID + "; ";

    ResultSet resultSet = null;
    Connection connection = null;
    PrintWriter writer = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      int i = rowIndexes.SERDE_ID;

      // this first writer should overwrite the previous runs
      writer = new PrintWriter(outputfile, "UTF-8");
      int ix = 0;
      writer.println(HiveSerde.ToSQLInsertPrefix());
      while (resultSet.next()) {
        ix++;
        i++;
        HiveSerde item = new HiveSerde(resultSet, i);
        map.put(item.SERDE_ID, i);
        boolean isFirst = (ix == 1);
        writer.println(item.ToSQLInsert(isFirst));
      }
      writer.println(";");
      i++;
      writer.println("UPDATE SEQUENCE_TABLE SET NEXT_VAL='" + i
          + "' WHERE SEQUENCE_NAME='org.apache.hadoop.hive.metastore.model.MSerDeInfo';");

    } finally {
      if (connection != null) {
        connection.close();
      }
      if (writer != null) {
        writer.close();
      }
    }
    return map;
  }

  public HashMap<Integer, Integer> GetSDs(HashMap<Integer, Integer> lookup) throws IOException, SQLException,
      ClassNotFoundException {

    HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

    String query = "Select SDS.* from SDS " + "join (PARTITIONS, TBLS) "
        + "ON (SDS.SD_ID = PARTITIONS.SD_ID AND PARTITIONS.TBL_ID = TBLS.TBL_ID) " + "where TBLS.TBL_ID ="
        + rowIndexes.SOURCE_TABLE_ID + "; ";

    ResultSet resultSet = null;
    Connection connection = null;
    PrintWriter writer = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      int i = rowIndexes.SD_ID;
      int new_tbl = rowIndexes.TARGET_TABLE_ID;
      int ix = 0;
      writer = new PrintWriter(new BufferedWriter(new FileWriter(outputfile, true)));
      writer.println(HiveSD.ToSQLInsertPrefix());
      while (resultSet.next()) {
        ix++;
        i++;
        HiveSD item = new HiveSD(resultSet, new_tbl, i, lookup);
        map.put(item.SD_ID, i);
        boolean isFirst = (ix == 1);
        writer.println(item.ToSQLInsert(isFirst));
      }
      writer.println(";");
      i++;
      writer.println("UPDATE SEQUENCE_TABLE SET NEXT_VAL='" + i
          + "' WHERE SEQUENCE_NAME='org.apache.hadoop.hive.metastore.model.MStorageDescriptor';");
    } finally {
      if (connection != null) {
        connection.close();
      }
      if (writer != null) {
        writer.close();
      }
    }
    return map;
  }

  public void GetSerdeParms(HashMap<Integer, Integer> serdemap) throws IOException, SQLException,
      ClassNotFoundException {

    String query = "Select SERDE_PARAMS.* from SERDE_PARAMS  "
        + "join (SERDES, SDS, PARTITIONS, TBLS) "
        + "ON ( SERDE_PARAMS.SERDE_ID = SERDES.SERDE_ID AND SDS.SERDE_ID = SERDES.SERDE_ID AND SDS.SD_ID = PARTITIONS.SD_ID AND PARTITIONS.TBL_ID = TBLS.TBL_ID) "
        + "where TBLS.TBL_ID =" + rowIndexes.SOURCE_TABLE_ID + "; ";

    ResultSet resultSet = null;
    Connection connection = null;
    PrintWriter writer = null;
    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(query);

      writer = new PrintWriter(new BufferedWriter(new FileWriter(outputfile, true)));
      writer.println(HiveSerdeParam.ToSQLInsertPrefix());
      int ix = 0;
      while (resultSet.next()) {
        ix++;
        HiveSerdeParam item = new HiveSerdeParam(resultSet, serdemap);
        boolean isFirst = (ix == 1);
        writer.println(item.ToSQLInsert(isFirst));
      }
      writer.println(";");
    } finally {
      if (connection != null) {
        connection.close();
      }
      if (writer != null) {
        writer.close();
      }
    }
  }

  public HiveRowIndexes GetTableIndexes() throws IOException, ClassNotFoundException, SQLException {
    HiveRowIndexes indexes = new HiveRowIndexes();
    indexes.SOURCE_TABLE_ID = this.GetTableId(sourceTableName);
    indexes.TARGET_TABLE_ID = this.GetTableId(targetTableName);
    indexes.PART_ID = this.GetPartitionIndexId();
    indexes.SERDE_ID = this.GetSerdeIndexId();
    indexes.SD_ID = this.GetSDIndexId();
    return indexes;
  }

}
