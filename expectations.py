import streamlit as st
import pandas as pd
import yaml
from io import StringIO
import os.path
import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from pprint import pprint

st.set_page_config(layout='wide')
uploaded_file = None

# Define all dbt-expectations rules and their parameters
# TODO Optionals e.g. Group by, Step, is raw, flags, compare_group_by etc
DBT_RULES = {
    "dbt_expectations.expect_column_to_exist": ["column"],
    "dbt_expectations.expect_column_values_to_be_unique": ["column"],
    "dbt_expectations.expect_column_values_to_not_be_null": ["column"],
    "dbt_expectations.expect_column_values_to_be_in_type_list": ["column", "column_type_list"],
    "dbt_expectations.expect_column_values_to_match_regex": ["column", "regex"],
    "dbt_expectations.expect_column_values_to_not_match_regex": ["column", "regex"],
    "dbt_expectations.expect_column_values_to_be_in_set": ["column", "value_set"],
    "dbt_expectations.expect_column_values_to_not_be_in_set": ["column", "value_set"],
    "dbt_expectations.expect_column_values_to_be_between": ["column", "min_value", "max_value", "strict_min", "strict_max"],
    "dbt_expectations.expect_column_value_lengths_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_value_lengths_to_equal": ["column", "value"],
    "dbt_expectations.expect_column_median_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_mean_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_min_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_max_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_sum_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_pair_values_a_to_be_greater_than_b": ["column_A", "column_B", "or_equal"],
    "dbt_expectations.expect_column_pair_values_to_be_in_set": ["column_A", "column_B", "value_set"],
    "dbt_expectations.expect_compound_columns_to_be_unique": ["column_list"],
    "dbt_expectations.expect_multicolumn_sum_to_be_between": ["column_list", "min_value", "max_value"],
    "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart": [
        "date_column",
        "n",
        "datepart",
        "interval",
        "test_start_date",
        "test_end_date",
        "row_condition",
        "exclusion_condition",
    ],
    "dbt_expectations.expect_table_row_count_to_be_between": ["min_value", "max_value"],
    "dbt_expectations.expect_table_column_count_to_be_between": ["min_value", "max_value"],
    "dbt_expectations.expect_table_columns_to_match_ordered_list": ["column_list"],
    "dbt_expectations.expect_table_row_count_to_equal_other_table": ["other_table"],
    "dbt_expectations.expect_column_values_to_match_like_pattern": ["column", "like_pattern"],
    "dbt_expectations.expect_column_values_to_not_match_like_pattern": ["column", "unlike_pattern"],
    "dbt_expectations.expect_column_values_to_match_like_pattern_list": ["column", "like_pattern_list"],
    "dbt_expectations.expect_column_values_to_not_match_like_pattern_list": ["column", "unlike_pattern_list"],
    "dbt_expectations.expect_column_stdev_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_proportion_of_unique_values_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_most_common_value_to_be_in_set": ["column", "value_set"],
    "dbt_expectations.expect_column_least_common_value_to_be_in_set": ["column", "value_set"],
    "dbt_expectations.expect_column_most_common_value_to_match_regex": ["column", "regex"],
    "dbt_expectations.expect_column_chisquare_test_p_value_to_be_greater_than": ["column", "value_set", "p"],
    "dbt_expectations.expect_column_pair_cramers_phi_value_to_be_less_than": ["column_A", "column_B", "threshold"],
    "dbt_expectations.expect_column_kl_divergence_to_be_less_than": ["column", "partition_object", "threshold"],
    "dbt_expectations.expect_table_to_contain_column_list": ["column_list"],
    "dbt_expectations.expect_table_row_count_to_be_greater_than": ["min_value"],
    "dbt_expectations.expect_table_row_count_to_be_less_than": ["max_value"],
    "dbt_expectations.expect_table_row_count_to_be_equal_to": ["value"],
    "dbt_expectations.expect_table_row_count_to_be_nonzero": [],
    "dbt_expectations.expect_table_to_have_no_duplicate_rows": ["column_list"],
    "dbt_expectations.expect_table_columns_to_match_set": ["column_set"],
    "dbt_expectations.expect_table_columns_to_be_subset_of": ["column_set"],
}

def separate_rules(rules: list) -> tuple[list,dict]: # returns a tuple of table rules and column rules 
    table_rules = []
    column_rules = {}
    for rule in rules:
        if 'column' not in rule.keys(): # if rule is a table rule
            table_rules.append(rule)
        else: 
            cols_wo_keys = rule.copy()
            cols_wo_keys.pop('column')
            rule_name = cols_wo_keys.pop('test')
            if len(cols_wo_keys) == 0:
                if rule['column'] in column_rules.keys(): # if column already exists in column_rules
                    column_rules[rule['column']].append(rule_name)
                else:
                    column_rules[rule['column']] = [rule_name]
            else:
                if rule['column'] in column_rules.keys(): # if column already exists in column_rules
                    column_rules[rule['column']].append({rule_name: cols_wo_keys})
                else:
                    column_rules[rule['column']] = [{rule_name: cols_wo_keys}]
    return table_rules, column_rules

# Helper function to generate YAML
# TODO Ensure the format matches the working model definition
def generate_yaml(table_name=None):
    table_rules, column_rules = separate_rules(st.session_state["tests"])
    if table_name is None:
        table_name = os.path.splitext(uploaded_file.name)[0]
    for col, rules in column_rules.items():
        print(f' Column is {col} rules are {rules}')

    yaml_doc = {
        'version': 2,
        'models': [
            {
                'name': table_name,
                'description': 'Synthesised rules from DQ rule builder',
                'tests': table_rules if table_rules else None,
                'columns': [{'name': col, 'tests': rules} for col, rules in column_rules.items()] if column_rules else None
            }
        ]
    }
    # Remove None values
    yaml_doc['models'][0] = {k: v for k, v in yaml_doc['models'][0].items() if v is not None}
    return yaml.dump(yaml_doc, sort_keys=False)

# Helper function to parse YAML
def parse_yaml(yaml_content):
    try:
        return yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        st.error(f"Error parsing YAML: {e}")
        return None

def format_rule_nicely(option):
    replacements = {
        'dbt_expectations.': '',
        '_': ' ',
        'dbt_expectations\\.': ''
    }
    for old, new in replacements.items():
        option = option.replace(old, new)
    return option.capitalize()

def dbt_command(command: str, model_name: str):
    # initialize
    dbt = dbtRunner()
    # create CLI args as a list of strings
    cli_args = [command, "--select", model_name, "--project-dir", "expectations"]
    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)
    # inspect the results
    results = []
    for r in res.result:
        pprint(f"--Result {r}")
        if r.status == 'pass':
            icon = '✅'
        else:
            icon = '❌'
        results.append({'Name': r.node.name, 'Result' : icon if command == 'test' else r.status, 'Execution Time': r.execution_time})
    st.dataframe(pd.DataFrame(results),hide_index=True, width=1000) 

st.title("Data Quality Rule Builder")

data, define, view, exec_tests = st.tabs(['Data', 'Define Rules', 'View Rules', 'Execute Tests'])

with data:
    uploaded_file = st.file_uploader("Upload CSV or Excel file", type=["csv", "xls", "xlsx"])
    if uploaded_file:
        try:
            if uploaded_file.name.endswith(".csv"):
                data = pd.read_csv(uploaded_file)
            else:
                data = pd.read_excel(uploaded_file)
            
            st.write("Preview of uploaded file:")
            st.dataframe(data.head())
        except Exception as e:
            st.error(f"Error reading file: {e}")
    existing_rules_yaml = st.file_uploader("Reload Existing Rules (YAML)", type=["yaml", "yml"])
    rules = []
    if existing_rules_yaml:
        rules = parse_yaml(existing_rules_yaml.read().decode())["expectations"] 

    with define:
        if uploaded_file:    
            headers = list(data.columns)
            
            st.write("## Define Quality Rules")


            
            if "tests" not in st.session_state:
                st.session_state["tests"] = rules
            
            selected_rule = st.selectbox("Select a Rule", 
                                        list(DBT_RULES.keys()), 
                                        format_func=format_rule_nicely)
            selected_parameters = DBT_RULES[selected_rule]
            
            rule_params = {}
            for param in selected_parameters:
                match param:
                    case "column":
                        rule_params[param] = st.selectbox(f"Select {param}", headers)
                    case "column_A" | "column_B":
                        rule_params[param] = st.selectbox(f"Select {param}", headers)
                    case 'max_value' | 'min_value':
                        rule_params[param] = st.number_input(f"Enter {param}", step=0.01)
                    case "regex":
                        rule_params[param] = st.text_input(f"Enter {param}")
                    case "value_set" | "like_pattern_list" | "unlike_pattern_list" | "column_type_list":
                        value_set = st.text_area(f"Enter {param} (comma-separated values)")
                        rule_params[param] = [v.strip() for v in value_set.split(",") if v.strip()]
                    case "column_list":
                        column_list = st.multiselect(f"Select {param}", headers)
                        rule_params[param] = column_list
                    case "date_column":
                        rule_params[param] = st.selectbox(f"Select {param}", headers)
                    case "n":
                        rule_params[param] = st.number_input(f"Enter {param} (e.g., days, months)", step=1)
                    case "datepart":
                        rule_params[param] = st.selectbox(f"Select {param}", ["day", "month", "year"])
                    case "interval":
                        rule_params[param] = st.number_input(f"Enter {param} (e.g., every N intervals)", step=1)
            
            row_condition = st.text_input(f"Enter row condition", placeholder='Enter a condition that will limit the rows this rule is applied to')
            if len(row_condition) > 0:
                rule_params['row_condition'] = row_condition
            strictly = st.checkbox(f"Strict? See https://github.com/calogica/dbt-expectations/tree/0.10.4/?tab=readme-ov-file", value=True)
            
            if not strictly:
                rule_params['strictly'] = False

            if st.button("Add Rule"):
                st.session_state["tests"].append({"test": selected_rule, **rule_params})

        with view:
            if uploaded_file:    
                st.write("### Defined Rules")
                for idx, rule in enumerate(st.session_state["tests"]):
                    st.write(f"Test {idx + 1}: {rule}")
                    if st.button(f"Remove Test {idx + 1}", key=f"remove_{idx}"):
                        st.session_state["tests"].pop(idx)
                
                st.write("## Export Rules to YAML")
                yaml_output = generate_yaml()
                st.code(yaml_output, language="yaml")
                st.download_button("Download YAML", yaml_output, "rules.yaml", "text/yaml")
        
        with exec_tests:
            if uploaded_file:
                st.write("### Execute Tests")
                model_name = st.text_input("Enter table name", value="my_table")
                csv_file = f'{model_name}.csv'
                dbt_test = st.button("Execute Tests")
                prog = st.progress(0)
                if dbt_test:
                    prog.progress(0.1,text='Saving uploaded data...')
                    b = uploaded_file.getvalue()
                    with open(f'{csv_file}', "wb") as f:
                        f.write(b)
                    prog.progress(0.2,text='Saving SQL...')
                    sql = f"SELECT * FROM read_csv_auto('{csv_file}')"
                    with open(f'expectations/models/example/{model_name}.sql', "w") as f:
                        f.write(sql)
                    prog.progress(0.3,text='Saving YAML...')              
                    # Save the yaml file with the rules
                    yaml = generate_yaml(model_name)
                    with open(f'expectations/models/example/{model_name}.yml', "w") as f:
                        f.write(yaml)
                    prog.progress(0.4,text='Loading the model...')
                    dbt_command("run", model_name)
                    prog.progress(0.9,text='Testing the model...')
                    res = dbt_command("test", model_name)


