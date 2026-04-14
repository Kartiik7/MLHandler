import pandas as pd
import numpy as np

from app.services.type_fixer import fix_types


def test_unit_extraction_gb():
	"""Test extraction of numeric values from unit strings like '8GB'."""
	df = pd.DataFrame({"storage": ["8GB", "16GB", "32GB"]})
	fixed, stats = fix_types(df)
	
	assert stats["conversions"]["storage"]["action"] == "unit_extraction"
	assert fixed["storage"].iloc[0] == 8.0
	assert fixed["storage"].iloc[1] == 16.0
	assert fixed["storage"].iloc[2] == 32.0


def test_unit_extraction_kg():
	"""Test extraction of numeric values from unit strings like '1.83kg'."""
	df = pd.DataFrame({"weight": ["1.83kg", "2.5kg", "0.75kg"]})
	fixed, stats = fix_types(df)
	
	assert stats["conversions"]["weight"]["action"] == "unit_extraction"
	assert abs(fixed["weight"].iloc[0] - 1.83) < 0.01
	assert abs(fixed["weight"].iloc[1] - 2.5) < 0.01
	assert abs(fixed["weight"].iloc[2] - 0.75) < 0.01


def test_unit_extraction_with_space():
	"""Test extraction from '512GB SSD' format."""
	df = pd.DataFrame({"spec": ["512GB SSD", "256GB SSD", "1TB HDD"]})
	fixed, stats = fix_types(df)
	
	# Should extract the numeric part
	assert stats["conversions"]["spec"]["action"] == "unit_extraction"
	assert fixed["spec"].iloc[0] == 512.0
	assert fixed["spec"].iloc[1] == 256.0
	assert fixed["spec"].iloc[2] == 1.0


def test_unit_extraction_mixed():
	"""Test mixed data where some have units and some don't."""
	df = pd.DataFrame({"mixed": ["8GB", "plain text", "16GB", None]})
	fixed, stats = fix_types(df)
	
	# Should extract where possible
	assert fixed["mixed"].iloc[0] == 8.0
	assert pd.isna(fixed["mixed"].iloc[1])  # can't extract from plain text
	assert fixed["mixed"].iloc[2] == 16.0
	assert pd.isna(fixed["mixed"].iloc[3])


def test_convert_types_disabled():
	"""Test that convert_types=False skips all conversions."""
	df = pd.DataFrame({"storage": ["8GB", "16GB"]})
	config = {"convert_types": False}
	fixed, stats = fix_types(df, config)
	
	assert stats["conversions"]["storage"]["action"] == "skipped"
	assert fixed["storage"].iloc[0] == "8GB"  # unchanged
	assert fixed["storage"].iloc[1] == "16GB"
