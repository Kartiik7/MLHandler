import pytest
import pandas as pd

from app.services.loader import load_csv


def test_valid_csv_load(tmp_path):
	p = tmp_path / "good.csv"
	p.write_text("a,b\n1,2\n3,4", encoding="utf-8")

	df, meta = load_csv(str(p))
	assert isinstance(df, pd.DataFrame)
	assert meta["rows"] == 2
	assert meta["columns"] == 2
	assert meta["encoding"] in ("utf-8", "latin-1")


def test_empty_file_raises(tmp_path):
	p = tmp_path / "empty.csv"
	p.write_text("", encoding="utf-8")

	with pytest.raises(ValueError) as ei:
		load_csv(str(p))
	assert "empty" in str(ei.value).lower()


def test_invalid_format_raises(tmp_path):
	# Write content that will cause the CSV parser to fail (unterminated quote)
	p = tmp_path / "bad.csv"
	p.write_text('"', encoding="utf-8")

	with pytest.raises(ValueError) as ei:
		load_csv(str(p))

	# Loader should report it tried encodings and include last error
	assert "Failed to read CSV; tried encodings" in str(ei.value)
