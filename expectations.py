import streamlit as st
import pandas as pd
import yaml
from io import StringIO
import os.path


st.set_page_config(layout='wide')
uploaded_file = None

# Define all dbt-expectations rules and their parameters
# TODO Optionals e.g. Group by, Step, is raw, flags, compare_group_by etc
DBT_RULES = {
    "dbt_expectations.expect_column_to_exist": ["column"],
    "dbt_expectations.expect_column_values_to_be_unique": ["column"],
    "dbt_expectations.expect_column_values_to_not_be_null": ["column"],
    "dbt_expectations.expect_column_values_to_be_in_type_list": ["column", "type_list"],
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

# Helper function to generate YAML
# TODO Ensure the format matches the working model definition
def generate_yaml():
    yaml_doc = {    'version': 2, 
                    'models': [
                        {'name': uploaded_file.name.replace(".csv",''),
                         'description': 'Synthesised rules from DQ rule builder',
                         'config': {'materialized': True},
                         'columns': st.session_state["rules"]
                        }
                    ]
                }
    return yaml.dump(yaml_doc, sort_keys=False)

# Helper function to parse YAML
def parse_yaml(yaml_content):
    return yaml.safe_load(yaml_content)

def format_rule_nicely(option):
    x = str(option).replace('dbt_expectations.','')    
    x = x.replace('_',' ')
    x = x.replace('dbt_expectations\.','')    
    x = x.capitalize()
    return x

st.title("Data Quality Rule Builder")

data, define, view = st.tabs(['Data', 'Define Rules', 'View Rules'])

with data:
    uploaded_file = st.file_uploader("Upload CSV or Excel file", type=["csv", "xls", "xlsx"])
    if uploaded_file:    
            if uploaded_file.name.endswith(".csv"):
                data = pd.read_csv(uploaded_file)
            else:
                data = pd.read_excel(uploaded_file)
            
            st.write("Preview of uploaded file:")
            st.dataframe(data.head())
            rules = []
            existing_rules_yaml = st.file_uploader("Reload Existing Rules (YAML)", type=["yaml", "yml"])

    with define:
        if uploaded_file:    
            headers = list(data.columns)
            
            st.write("## Define Quality Rules")

            if existing_rules_yaml:
                rules = parse_yaml(existing_rules_yaml.read().decode())["expectations"]
            
            if "rules" not in st.session_state:
                st.session_state["rules"] = rules
            
            selected_rule = st.selectbox("Select a Rule", 
                                        list(DBT_RULES.keys()), 
                                        format_func=format_rule_nicely)
            selected_parameters = DBT_RULES[selected_rule]
            
            rule_params = {}
            for param in selected_parameters:
                if param == "column":
                    rule_params[param] = st.selectbox(f"Select {param}", headers)
                elif param in ["column_A", "column_B"]:
                    rule_params[param] = st.selectbox(f"Select {param}", headers)
                elif param.endswith("_value"):
                    rule_params[param] = st.number_input(f"Enter {param}", step=0.01)
                elif param == "regex":
                    rule_params[param] = st.text_input(f"Enter {param}")
                elif param in ["value_set", "type_list", "like_pattern_list", "unlike_pattern_list"]:
                    value_set = st.text_area(f"Enter {param} (comma-separated values)")
                    rule_params[param] = [v.strip() for v in value_set.split(",") if v.strip()]
                elif param == "column_list":
                    column_list = st.multiselect(f"Select {param}", headers)
                    rule_params[param] = column_list
                elif param == "date_column":
                    rule_params[param] = st.selectbox(f"Select {param}", headers)
                elif param == "n":
                    rule_params[param] = st.number_input(f"Enter {param} (e.g., days, months)", step=1)
                elif param == "datepart":
                    rule_params[param] = st.selectbox(f"Select {param}", ["day", "month", "year"])
                elif param == "interval":
                    rule_params[param] = st.number_input(f"Enter {param} (e.g., every N intervals)", step=1)
            
            rule_params['row_condition'] = st.text_input(f"Enter row condition", placeholder='Enter a condition that will limit the rows this rule is applied to')
            rule_params['strictly'] = st.checkbox(f"Strict? See https://github.com/calogica/dbt-expectations/tree/0.10.4/?tab=readme-ov-file")


            if st.button("Add Rule"):
                st.session_state["rules"].append({"rule": selected_rule, **rule_params})
        with view:
            if uploaded_file:    
                st.write("### Defined Rules")
                for idx, rule in enumerate(st.session_state["rules"]):
                    st.write(f"Rule {idx + 1}: {rule}")
                    if st.button(f"Remove Rule {idx + 1}", key=f"remove_{idx}"):
                        st.session_state["rules"].pop(idx)
                
                st.write("## Export Rules to YAML")
                yaml_output = generate_yaml()
                st.code(yaml_output, language="yaml")
                st.download_button("Download YAML", yaml_output, "rules.yaml", "text/yaml")