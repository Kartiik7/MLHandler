import pandas as pd
import numpy as np

from app.services.cleaner import clean_dataframe


def test_missing_value_fill():
	df = pd.DataFrame({
		"num": [1, None, 3],
		"cat": ["a", None, "a"],
	})

	cleaned, stats = clean_dataframe(df)

	# Numeric median imputation
	assert "num" in stats["imputed"]
	assert stats["imputed"]["num"] == 1
	# median of [1,3] is 2
	assert float(cleaned["num"].iloc[1]) == 2.0

	# Categorical mode imputation
	assert "cat" in stats["imputed"]
	assert stats["imputed"]["cat"] == 1
	assert cleaned["cat"].iloc[1] == "a"


def test_duplicate_removal():
	df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
	cleaned, stats = clean_dataframe(df)
	assert stats["duplicates_removed"] == 1
	assert stats["after_rows"] == 2


def test_whitespace_trimming():
	df = pd.DataFrame({"s": ["  hello  ", " world\t", None]})
	cleaned, stats = clean_dataframe(df)

	# Ensure leading/trailing whitespace removed for non-null values
	non_null = cleaned["s"].dropna().tolist()
	for v in non_null:
		assert v == v.strip()


def test_drop_all_null_column():
	df = pd.DataFrame({"allnull": [None, None], "v": [1, 2]})
	cleaned, stats = clean_dataframe(df)
	assert "allnull" in stats["dropped_columns"]
	assert "allnull" not in cleaned.columns


def test_fill_missing_numeric_mean():
	"""Test that fill_missing_numeric='mean' uses mean instead of median."""
	df = pd.DataFrame({"num": [1, None, 3, 4]})
	config = {"fill_missing_numeric": "mean"}
	cleaned, stats = clean_dataframe(df, overrides=config)
	
	# mean of [1,3,4] is 8/3 = 2.666...
	assert "num" in stats["imputed"]
	assert abs(cleaned["num"].iloc[1] - (8/3)) < 0.01


def test_fill_missing_numeric_median():
	"""Test that fill_missing_numeric='median' uses median (default)."""
	df = pd.DataFrame({"num": [1, None, 3, 4]})
	config = {"fill_missing_numeric": "median"}
	cleaned, stats = clean_dataframe(df, overrides=config)
	
	# median of [1,3,4] is 3
	assert "num" in stats["imputed"]
	assert cleaned["num"].iloc[1] == 3.0


def test_drop_duplicates_false():
	"""Test that drop_duplicates=False keeps duplicate rows."""
	df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
	config = {"drop_duplicates": False}
	cleaned, stats = clean_dataframe(df, overrides=config)
	
	assert stats["duplicates_removed"] == 0
	assert stats["after_rows"] == 3  # all rows kept


def test_trim_strings_false():
	"""Test that trim_strings=False keeps whitespace intact."""
	df = pd.DataFrame({"s": ["  hello  ", " world\t"]})
	config = {"trim_strings": False}
	cleaned, stats = clean_dataframe(df, overrides=config)
	
	# Whitespace should be preserved
	assert cleaned["s"].iloc[0] == "  hello  "
	assert cleaned["s"].iloc[1] == " world\t"


def test_drop_empty_columns_false():
	"""Test that drop_empty_columns=False keeps all-null columns (but they may get filled)."""
	df = pd.DataFrame({"allnull": [None, None], "v": [1, 2]})
	config = {"drop_empty_columns": False, "fill_missing_categorical": "none"}
	cleaned, stats = clean_dataframe(df, overrides=config)
	
	# Column should be kept even though it's all null
	assert "allnull" in cleaned.columns
	# With fill strategy 'none', it stays null
