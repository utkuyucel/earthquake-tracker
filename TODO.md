1 - Check for the revised versions for each run.

2 - Upload the output delta to postgres, using docker for postgres.

3 - Run fastapi as API reading from postgres.

4 - Add a detailed retry mechanism, especially on earthquake time; the website is returning 503.

5 - Add multi support, and generate seperate extractors for different sources. Thans standardize the columns into one for postgres so go with "source" column in the db.