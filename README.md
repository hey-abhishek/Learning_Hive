
# Hive query and command 
  create database hive_class_b1;
  use hive_class_b1;

create table department_data                                                                                                            
    > (                                                                                                                                       
    > dept_id int,                                                                                                                            
    > dept_name string,                                                                                                                       
    > manager_id int,                                                                                                                         
    > salary int)                                                                                                                             
    > row format delimited                                                                                                                    
    > fields terminated by ','; 
    
describe department_data;

describe formatted department_data;

# For data load from local
load data local inpath 'file:///tmp/hive_class/depart_data.csv' into table department_data; 

# Display column name
set hive.cli.print.header = true;

# Load data from hdfs location
load data inpath '/tmp/hive_data_class_2/' into table department_data_from_hdfs;


# Create external table 

create external table department_data_external                                                                                          
    > (                                                                                                                                       
    > dept_id int,                                                                                                                            
    > dept_name string,                                                                                                                       
    > manager_id int,                                                                                                                         
    > salary int                                                                                                                              
    > )                                                                                                                                       
    > row format delimited                                                                                                                    
    > fields terminated by ','                                                                                                                
    > location '/tmp/hive_data_class_2/'; 
    
    
    
# work with Array data types

create table employee                                                                                                                   
    > (                                                                                                                                       
    > id int,                                                                                                                                 
    > name string,                                                                                                                            
    > skills array<string>                                                                                                                    
    > )                                                                                                                                       
    > row format delimited                                                                                                                    
    > fields terminated by ','                                                                                                                
    > collection items terminated by ':';                                                                                                     

load data local inpath 'file:///tmp/hive_class/array_data.csv' into table employee; 


# Get element by index in hive array data type

select id, name, skills[0] as prime_skill from employee;

select                                                                                                                                  
    > id,                                                                                                                                     
    > name,                                                                                                                                   
    > size(skills) as size_of_each_array,                                                                                                     
    > array_contains(skills,"HADOOP") as knows_hadoop,                                                                                        
    > sort_array(skills) as sorted_array                                                                                                                     
    > from employee; 
    
    
# table for map data

create table employee_map_data                                                                                                          
    > (                                                                                                                                       
    > id int,                                                                                                                                 
    > name string,                                                                                                                            
    > details map<string,string>                                                                                                              
    > )                                                                                                                                       
    > row format delimited                                                                                                                    
    > fields terminated by ','                                                                                                                
    > collection items terminated by '|'                                                                                                      
    > map keys terminated by ':';
    
 load data local inpath 'file:///tmp/hive_class/map_data.csv' into table employee_map_data;
 
 select                                                                                                                                  
    > id,                                                                                                                                     
    > name,                                                                                                                                   
    > details["gender"] as employee_gender                                                                                                    
    > from employee_map_data; 
 
 # map functions
 select                                                                                                                                  
    > id,                                                                                                                                     
    > name,                                                                                                                                   
    > details,                                                                                                                                
    > size(details) as size_of_each_map,                                                                                                      
    > map_keys(details) as distinct_map_keys,                                                                                                 
    > map_values(details) as distinct_map_values                                                                                              
    > from employee_map_data; 



# Assignment Dataset

https://www.kaggle.com/datasets/imdevskp/corona-virus-repor
 
#creating database    
hive> create table covid_data
    
#creating table of covid_data 
    
hive> create  table covid_data
    > (
    > state string,
    > country string,
    > lat float,
    > Long float,
    > date int,
    > confirmed int,
    > deaths int,
    > recover int,
    > active int,
    > who_region string
    > )
    > row format delimited
    > fields terminated by ',';
    
#load data from local 
    
    load data local inpath 'file:///home/cloudera/home/covid_19_clean_complete.csv' into table covid_data
    
#creating tmp

     hadoop fs -mkdir /tmp/covid_19_data_2
     hadoop fs -put /home/cloudera/data/covid_19_clean_complete.csv /tmp/covid_19_data_2
     hadoop fs -ls /tmp/covid_19_data_2
   
 #load data from hdfs
    
    load data inpath '/tmp/covid_19_data_2/' into table department_data_from_hdfs;
    
    
# creating External table 
    
    
hive> create external table covid_19_external_data
    > (
    > state string,
    > country string,
    > lat float,
    > Long float,
    > date int,
    > confirmed int,
    > deaths int,
    > recover int,
    > active int,
    > who_region string
    > )
    > row format delimited
    > fields terminated by ','
    > location '/tmp/covid_19_data_2/';

    
    
    
    
