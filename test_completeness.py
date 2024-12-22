from rules import DBT_RULES
from expectations import build_form_from_parameters

for rule, params in DBT_RULES.items():
    try:
        build_form_from_parameters(['a','b'], params)
    except Exception as e:
        print(f"Rule {rule} failed")
        continue