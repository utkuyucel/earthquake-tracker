- Add a scheduler with airflow. Scrap it every 5 mins.

- Run fastapi as API reading from postgres. 

- Add a detailed retry mechanism, especially on earthquake time; BOUN the website is returning 503.

- Add multi source support, and generate seperate extractors for different sources. Thans standardize the columns into one for postgres so go with "source" column in the db.
    Bcs in future, we're not only gonna scrap from one website.

- Check for the revised versions for each run.
    i.e: Some earthquakes can be revised. So a 4.2 Magnitute eq can be revised as 4.5 or 3.9 too.

- Upload the data to postgres, using docker for postgres. 
    we need delta here (as setting, we can change this to full scrap too). for each scrap; only keep the new data instead inserting same data again and again, 
    so incremental.

