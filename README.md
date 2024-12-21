# Expectations

This project allows anyone to provide a test file in csv or excel format that can be used to define tests suitable for execution using dbt-expectations. The file is used to determine columns available for testing and test data to run the expectations against.

The app runs using streamlit 

`streamlit run expectations.py`

this will open the app in a browser.

## WIP instructions 
1. Upload a file
2. Set expectations using the interface
3. Save the yml file in `expectations\models\example\[mymodel].yml`
4. Save the data in the root of the project as [mymodel].csv (or xls)
5. Create a file [mymodel].sql with contents of the form
   `SELECT *
    FROM read_csv_auto('../[mymodel].csv')
   `
6. run `dbt init` in the project root
7. run `dbt deps` to pull the expectations and duckdb dependencies 
8. run `dbt run` to create the data in duckdb database
9. run `dbt test` to execute the tests against the data in the database
