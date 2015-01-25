package com.amazonaws.services.emr.copyhivepartitions;



import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class Main {

  @SuppressWarnings("static-access")
  public static void main(String args[]) {  
    
    Options opt = new Options();
    try {
      opt.addOption("h", "help", false, "Print help for this application");
      opt.addOption("e", "example", false, "java -jar copyhivepartitions-0.0.1-SNAPSHOT.jar  -c jdbc:mysql://emrdb.cz8lzaalmbmy.us-east-1.rds.amazonaws.com:3306/hive -u root -p password -s my_old_table -t my_new_table  -o insert.sql");
      opt.addOption(OptionBuilder.withArgName("database.url")
          .withDescription("the mysql connection string. e.g [jdbc:mysql://emrdb.cz8lzaalmbmy.us-east-1.rds.amazonaws.com:3306/hive]")
          .hasArg()
          .isRequired(true)
          .create("c"));
      
      opt.addOption(OptionBuilder.withArgName("database.username")
          .withDescription("the username for connecting to the database")
          .hasArg().isRequired(true)
          .create("u"));
      
      opt.addOption(OptionBuilder.withArgName("database.password")
          .withDescription("the password for the mysql user")
          .hasArg()
          .isRequired(true)
          .create("p"));
      opt.addOption(OptionBuilder.withArgName("sourceTableName")
          .withDescription("The name of the source table that the partitions will be copied from")
          .hasArg()
          .isRequired(true)
          .create("s"));
      opt.addOption(OptionBuilder.withArgName("targetTableName")
          .withDescription("The name of the destination table that the partitions will be copied into")
          .hasArg()
          .isRequired(true)
          .create("t"));
      opt.addOption(OptionBuilder.withArgName("output.file")
          .withDescription("The name of the output file")
          .hasArg()
          .isRequired(false)
          .create("o"));
      

    
      BasicParser parser = new BasicParser();
      CommandLine cl = parser.parse(opt, args);
      Properties props = new Properties(); 
      props.put("database.url", cl.getOptionValue("c"));
      props.put("database.username", cl.getOptionValue("u"));
      props.put("database.password", cl.getOptionValue("p"));
      props.put("sourceTableName", cl.getOptionValue("s"));
      props.put("targetTableName", cl.getOptionValue("t"));
      props.put("database.driverClassName", "com.mysql.jdbc.Driver");
      
      if (cl.hasOption("o")) {
        props.put("output.file", cl.getOptionValue("o"));
      }
      else {
        props.put("output.file", cl.getOptionValue("output.sql"));
      }
      InsertComposer.Run(props);
     
    } catch (ParseException e) {
      System.out.println("");
      System.out.println(e.getMessage());
      System.out.println("");
      HelpFormatter f = new HelpFormatter();
      f.printHelp("HivePartitionCopier", opt);
    }
  }
  

}
