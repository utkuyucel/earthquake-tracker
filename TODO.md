v0.2:

    1 - Upload the data to postgres, using docker for postgres. 

    2 - Act the first layer as dwh, call it bronze layer.
        * I want the bronze layer as only non-duplicate value implemented. So If there's already same rows with same values, log and skip it. If there's some best practice and embedding way of doing that,
        you can implement it. Otherwise let's go with checking the hash of full values of that row for comparing so if hash is changed, it changed

        * I want the output of the bronze layer as non-duplicate values with inserted_at column (and hash_column if needed). inserted_at is current utc timestamp which will be helping us for delta in downstream models

    3 - Second layer, which is silver layer:
        * I want it as only get the latest inserted at of same values for duplicates if there's any (theoratically, there shouldn't be cuz we're already filtering them out)
            
        * Also it should take care of the revised values. If each value of the column is same except inserted_at and magnitude, it means the value is revised later. So we need to filter the latest version of that earthquake


Future versions:

- Add a scheduler with airflow. Scrap it every 5 mins.

- Run fastapi as API reading from postgres. 

- Add a detailed retry mechanism, especially on earthquake time; BOUN the website is returning 503.

- Add multi source support, and generate seperate extractors for different sources. Thans standardize the columns into one for postgres so go with "source" column in the db.
    Bcs in future, we're not only gonna scrap from one website.

- Check for the revised versions for each run.
    i.e: Some earthquakes can be revised. So a 4.2 Magnitute eq can be revised as 4.5 or 3.9 too.



