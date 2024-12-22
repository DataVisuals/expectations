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
    "dbt_expectations.expect_table_columns_to_contain_set": ["column_list"],
    "dbt_expectations.expect_row_values_to_have_recent_data": ["column", "interval", "date_format"],
    "dbt_expectations.expect_grouped_row_values_to_have_recent_data": ["column", "interval", "date_format", "group_by_column"],
    "dbt_expectations.expect_table_aggregation_to_equal_other_table": ["aggregation_column", "other_table", "other_column"],
    "dbt_expectations.expect_table_column_count_to_equal_other_table": ["other_table"],
    "dbt_expectations.expect_table_columns_to_not_contain_set": ["column_set"],
    "dbt_expectations.expect_table_column_count_to_equal": ["value"],
    "dbt_expectations.expect_table_row_count_to_equal_other_table_times_factor": ["other_table", "factor"],
    "dbt_expectations.expect_table_row_count_to_equal": ["value"],
    "dbt_expectations.expect_column_values_to_be_null": ["column"],
    "dbt_expectations.expect_column_values_to_be_of_type": ["column", "type"],
    "dbt_expectations.expect_column_values_to_have_consistent_casing": ["column"],
    "dbt_expectations.expect_column_values_to_be_increasing": ["column"],
    "dbt_expectations.expect_column_values_to_be_decreasing": ["column"],
    "dbt_expectations.expect_column_values_to_match_regex_list": ["column", "regex_list"],
    "dbt_expectations.expect_column_values_to_not_match_regex_list": ["column", "regex_list"],
    "dbt_expectations.expect_column_distinct_count_to_equal": ["column", "value"],
    "dbt_expectations.expect_column_distinct_count_to_be_greater_than": ["column", "value"],
    "dbt_expectations.expect_column_distinct_count_to_be_less_than": ["column", "value"],
    "dbt_expectations.expect_column_distinct_values_to_be_in_set": ["column", "value_set"],
    "dbt_expectations.expect_column_distinct_values_to_contain_set": ["column", "value_set"],
    "dbt_expectations.expect_column_distinct_values_to_equal_set": ["column", "value_set"],
    "dbt_expectations.expect_column_distinct_count_to_equal_other_table": ["column", "other_table", "other_column"],
    "dbt_expectations.expect_column_quantile_values_to_be_between": ["column", "quantile", "min_value", "max_value", "group_by"],
    "dbt_expectations.expect_column_unique_value_count_to_be_between": ["column", "min_value", "max_value"],
    "dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B": ["column_A", "column_B", "or_equal"],
    "dbt_expectations.expect_column_pair_values_to_be_equal": ["column_A", "column_B"],
    "dbt_expectations.expect_select_column_values_to_be_unique_within_record": ["column_list"],
    "dbt_expectations.expect_multicolumn_sum_to_equal": ["column_list", "value"],
    "dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs": ["column", "n", "date_column_name", "period", "lookback_periods", "trend_periods"],
    "dbt_expectations.expect_column_values_to_be_within_n_stdevs": ["column", "n"],
}
