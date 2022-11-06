# Sparkify - Song Metadata Database

## ETL pipeline to read json format logs and load into PostgreSQL database for Sparkify

## DB design
### Sparkifydb is a star schema database, focused on the monitoring of user plays of songs, maintained in the songplays Fact table

### Fact tables
<ul>
    <li>songplays</li>
</ul>
### Dimension tables
<ul>
    <li>users</li>
    <li>songs</li>
    <li>artists</li>
    <li>time</li>
</ul>

## Running python scripts
<ol>
    <li>Initialise database - create_tables.py - entry point - create_tables.main</li>
    <ol>
        <li>open the terminal</li>
        <li>Run the command > python create_tables.py</li>
        <li>Run the command > python etl.py</li>
        <li>The terminal will show feedback as each file is loaded into sparkifydb</li>
    </ol>
    <li>Reads json data to insert into database - entry point - etl.main</li>
    <li>etl.main and create_tables.main refer to sql queries in sql_queries.py for each db interaction</li>
</ol>

## Files in Repository
<ul>
    <li>create_tables.py</li>
    <li>etl.py</li>
    <li>sql_queries.py</li>
    <li>data directory containing song_data and log_data subdirectories</li>
</ul>

## SQL Queries - contained in sql_queries.py
<ul>
    <li>song_select - requires an inner table join between songs and artists tables before select of song ID and artist ID</li>
    <li>Table Creation Queries - songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create</li>
    <li>Table Insertion Queries - songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert<li>
</ul>

