# Expectations


This project allows anyone to provide a test file in csv or excel format that can be used to define tests suitable for execution using dbt-expectations. The file is used to determine columns available for testing and test data to run the expectations against.

The app runs using streamlit 

`streamlit run expectations.py`

this will open the app in a browser.

## WIP instructions 
1. Upload a file
2. Set expectations using the interface
3. Run the tests 
4. Review the results

TODO

1. Type inference in duckdb will mean 0001 is inferred as text. 
2. YAML serialisation allows use of canonical form like this

```yaml
- Key
   - Element 1
   - Element 2
```
 and also 

```yaml
- Key
   - ['Element 1', 'Element 2']
```

It looks like dbt-expectations normally requires canonical form but not for some of the table level rules.