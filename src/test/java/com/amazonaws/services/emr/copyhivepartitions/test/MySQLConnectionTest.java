package com.amazonaws.services.emr.copyhivepartitions.test;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import com.amazonaws.services.emr.copyhivepartitions.*;

public class MySQLConnectionTest {
  static Logger logger = LogManager.getLogger(MySQLConnectionTest.class);
  

  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
   
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  
  @Test
  public void TestComposer(){
    //run from config file
    //InsertComposer.Run(null); 
  }
    
 
  @Test
  public void TestComposerMain(){
    //run from config file
    
    try {
      Properties props = new ConfigReader().getPropValuesFromFile();
      Main.main(new String[] {"-c", props.getProperty("database.url")
          ,"-u", props.getProperty("database.username")
          ,"-p", props.getProperty("database.password")
          ,"-s", props.getProperty("sourceTableName")
          ,"-t", props.getProperty("targetTableName")
          ,"-o", props.getProperty("output.file")});
      
    } catch (IOException e) {
      e.printStackTrace();
    }
   
  }
  
}
