import streamlit as st
import pandas as pd
import yaml
from io import StringIO
import os.path
import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from pprint import pprint
from rules import DBT_RULES

st.set_page_config(layout='wide')
uploaded_file = None

# TODO Define all dbt-expectations rules and their parameters
# TODO Optionals e.g. Group by, Step, is raw, flags, compare_group_by etc


def separate_rules(rules: list) -> tuple[dict,dict]: # returns a tuple of table rules and column rules 
    print(f"Trying to separate {rules}")
    table_rules = {}
    column_rules = {}
    for rule in rules:
        if 'column' not in rule.keys(): # if rule is a table rule
            table_rule = rule.copy()
            print(f'Table rule is {table_rule}')
            rule_name = table_rule.pop('test')
            table_rules[rule_name] = table_rule
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
                'tests': table_rules if table_rules else None,
                'columns': [{'name': col, 'tests': rules} for col, rules in column_rules.items()] if column_rules else None
            }
        ]
    }
    # Remove None values
    yaml_doc['models'][0] = {k: v for k, v in yaml_doc['models'][0].items() if v is not None}
    return yaml.dump(yaml_doc, sort_keys=False, default_flow_style=False)

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
        #pprint(f"--Result {r}")
        if r.status == 'pass':
            icon = '✅'
        else:
            icon = '❌'
        results.append({'Name': r.node.name, 'Result' : icon if command == 'test' else r.status, 'Execution Time': r.execution_time})
    st.dataframe(pd.DataFrame(results),hide_index=True, width=1000) 

def write_csv(uploaded_file, csv_file):
    b = uploaded_file.getvalue()
    with open(f'{csv_file}', "wb") as f:
        f.write(b)

def write_sql(model_name, csv_file):
    sql = f"SELECT * FROM read_csv_auto('{csv_file}')"
    with open(f'expectations/models/example/{model_name}.sql', "w") as f:
        f.write(sql)

def write_yaml(generate_yaml, model_name):
    yaml = generate_yaml(model_name)
    with open(f'expectations/models/example/{model_name}.yml', "w") as f:
        f.write(yaml)

def build_form_from_parameters(column_names, selected_parameters):
    rule_params = {}
    for param in selected_parameters:
        match param:
            case "column" | "column_A" | "column_B" | "group_by_column" | "date_column" | "date_column_name":
                rule_params[param] = st.selectbox(f"Select {param}", column_names)
            case 'max_value' | 'min_value' | "value" | "n" | "interval" | "lookback_periods" | "trend_periods" | "threshold" | "factor" | "p" | "sigma_threshold_upper" | "sigma_threshold_lower":
                rule_params[param] = st.number_input(f"Enter {param}", step=0.01)
            case "regex" | "regex_list" | "like_pattern" | "unlike_pattern" | "value_set" | "other_table" |  "exclusion_condition" | "period":
                rule_params[param] = st.text_input(f"Enter {param}")
            case "value_set" | "like_pattern_list" | "unlike_pattern_list" | "column_type_list":
                value_set = st.text_area(f"Enter {param} (comma-separated values)")
                rule_params[param] = [v.strip() for v in value_set.split(",") if v.strip()]
            case "column_list" | "column_set":
                column_list = st.multiselect(f"Select {param}", column_names)
                rule_params[param] = column_list
            case "datepart":
                rule_params[param] = st.selectbox(f"Select {param}", ["day", "month", "year"])
            case "test_start_date" | "test_end_date":
                rule_params[param] = st.date_input(f"Enter {param}")
            case "or_equal":
                rule_params[param] = st.checkbox(f"Check if {param} allows equal values alsoclea")
            case _:
                print(f"Parameter {param} not implemented")
                throw(NotImplementedError) # type: ignore
    return rule_params

def add_rule_to_session(selected_rule, rule_params):
    if st.session_state.get("tests", None) is None:
        st.session_state["tests"] = [{"test": selected_rule, **rule_params}]
    else:
        st.session_state["tests"].append({"test": selected_rule, **rule_params})

def display_defined_rules():
    st.write("### Defined Rules")
    for idx, rule in enumerate(st.session_state["tests"]):
        st.write(f"Test {idx + 1}: {rule}")
        if st.button(f"Remove Test {idx + 1}", key=f"remove_{idx}"):
            st.session_state["tests"].pop(idx)

def save_rules():
    st.write("## Save the rules")
    if st.session_state.get('model_name', None):
        yaml_output = generate_yaml(st.session_state.get('model_name'))
    else:
        yaml_output = generate_yaml()
    st.code(yaml_output, language="yaml")
    # st.download_button("Download YAML", yaml_output, "rules.yaml", "text/yaml")

#########################################################################################
# Streamlit UI
#########################################################################################

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
    with define:
        if uploaded_file:    
            column_names = list(data.columns)
            
            st.write("## Define Quality Rules")
            
            selected_rule = st.selectbox("Select a Rule", 
                                        list(DBT_RULES.keys()), 
                                        format_func=format_rule_nicely)
            selected_parameters = DBT_RULES[selected_rule]
            
            rule_params = build_form_from_parameters(column_names, selected_parameters)
            
            row_condition = st.text_input(f"Enter row condition", placeholder='Enter a condition that will limit the rows this rule is applied to')
            if len(row_condition) > 0:
                rule_params['row_condition'] = row_condition
            strictly = st.checkbox(f"Strict? See https://github.com/calogica/dbt-expectations/tree/0.10.4/?tab=readme-ov-file", value=True)
            
            if not strictly:
                rule_params['strictly'] = False

            if st.button("Add Rule"):
                add_rule_to_session(selected_rule, rule_params)

        with view:
            if uploaded_file and st.session_state.get("tests",None):    
                display_defined_rules()
                save_rules()
        
        with exec_tests:
            if uploaded_file:
                st.write("### Execute Tests")
                model_name = st.text_input("Enter table name", value="my_table")
                st.session_state['model_name']  = model_name
                csv_file = f'{model_name}.csv'
                dbt_test = st.button("Execute Tests")
                prog = st.progress(0)
                if dbt_test:
                    prog.progress(10,text='Saving uploaded data...')
                    write_csv(uploaded_file, csv_file)
                    prog.progress(20,text='Saving SQL...')
                    write_sql(model_name, csv_file)
                    prog.progress(30,text='Saving YAML...')              
                    # Save the yaml file with the rules
                    write_yaml(generate_yaml, model_name)
                    prog.progress(40,text='Loading the model...')
                    dbt_command("run", model_name)
                    prog.progress(90,text='Testing the model...')
                    res = dbt_command("test", model_name)
                    prog.progress(100,text='See results below.')


