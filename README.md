# HivePartitionCopier
Utility for copying Hive Partitions via direct connection to the Metastore Database.

The application assumes that the user has already done the: 

CREATE EXTERNAL TABLE <name> like <source> location Õs3://mybucket/folder/path'


# Building
git clone git@github.com:stmcpherson/HivePartitionCopier.git
cd HivePartitionCopier
mv /src/main/resources/config.properties.sample /src/main/resources/config.properties
--manually edit that file to point to your defaults


# Commandline usage
java -jar target/copyhivepartitions-0.0.1-SNAPSHOT.jar  -c jdbc:mysql://emrdb.cz8lzaalmbmy.us-east-1.rds.amazonaws.com:3306/hive -u root -p pass -s sourceTableName -t targetTablename  -o myfile.sql




