# ETL-Pipeline-PostgreSQL

## Project Description

The project is a music streaming application called Sparkify. They want to analyze the data and wants to know what songs user listening to. The application have dataset of songs and users activity. 

## Files Description

    - create_tables.py: When create_tables.py run, it will first create tables and drop if table already exists. 
    - etl.py: read and process data files
    - etl.ipyub: a jupyter notebook to analyse the dataset 
    - sql_queries: Contains the sql queries for dropping, creation, selection data from tables.
    - test.ipyub: Contains code to connects to database and see data in database. 
    

## Instruction to run

Steps to run the ETL pipeline 

- Step 1: run "create_tables.py" with ```python create_tables.py``` command. This will drop the existing database and tables. It will create new tables.

- Step 2: run 'etl.py' with  ```python etl.py``` command. This will run the ETL pipeline and process all the files



## Schema

#### Schema: 

##### Fact tables:

###### Songplays: 

​	*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

##### Dimension Tables:

###### users: 

​	*user_id, first_name, last_name, gender, level*

###### songs: 

​	*song_id, title, artist_id, year, duration*

###### artists: 

​	*artist_id, name, location, latitude, longitude*

###### time: 

​	*start_time, hour, day, week, month, year, weekday*

All the tables contains Primary Key as there should be something unique to identify the rows in the table.

In order to avoid conflicts while inserting, CONFLICT statement is also added.


Some preprocessing on artist_name and song_titles is also done to avoid potential errors 


I used etl.ipynb and test.ipynb for the testing and experiment purposes. 
