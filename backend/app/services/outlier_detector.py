"""Outlier detection utilities for MLHandler.

Provides functions to detect outliers in DataFrames using statistical methods.
"""
from typing import Dict, List
import pandas as pd
import numpy as np


def detect_outliers_iqr(df: pd.DataFrame) -> Dict[str, List[int]]:
    """Detect outliers in numeric columns using the IQR method.
    
    The IQR (Interquartile Range) method identifies outliers as values that fall
    outside the range [Q1 - 1.5*IQR, Q3 + 1.5*IQR], where:
    - Q1 is the first quartile (25th percentile)
    - Q3 is the third quartile (75th percentile)
    - IQR = Q3 - Q1
    
    Args:
        df: pandas DataFrame to analyze for outliers
        
    Returns:
        Dictionary where keys are numeric column names and values are lists
        of row indices (integers) where outliers were detected in that column.
        Columns with no outliers will have an empty list.
        
    Example:
        >>> df = pd.DataFrame({
        ...     'A': [1, 2, 3, 4, 100],
        ...     'B': ['x', 'y', 'z', 'w', 'v'],
        ...     'C': [10, 11, 12, 13, 14]
        ... })
        >>> outliers = detect_outliers_iqr(df)
        >>> outliers
        {'A': [4], 'C': []}
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    outliers = {}
    
    # Iterate through columns and process only numeric ones
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            # Get the column data, excluding NaN values for quartile calculation
            col_data = df[column]
            
            # Calculate quartiles and IQR
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            
            # Define outlier bounds
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Find indices where values are outliers (outside the bounds)
            # Note: NaN values are not considered outliers
            outlier_mask = (col_data < lower_bound) | (col_data > upper_bound)
            outlier_indices = df.index[outlier_mask].tolist()
            
            # Store the outlier indices for this column
            outliers[str(column)] = outlier_indices
    
    return outliers


def get_outliers_iqr(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Get outlier rows for each numeric column using the IQR method.
    
    This function identifies outliers using the IQR method and returns the actual
    DataFrame rows that contain outliers for each column. This is useful for
    previewing outliers before removing them.
    
    Args:
        df: pandas DataFrame to analyze for outliers
        
    Returns:
        Dictionary where keys are numeric column names and values are DataFrames
        containing only the rows where that column has outlier values.
        Columns with no outliers will have an empty DataFrame.
        
    Example:
        >>> df = pd.DataFrame({
        ...     'A': [1, 2, 3, 4, 100],
        ...     'B': [5, 6, 7, 8, 9],
        ...     'C': ['x', 'y', 'z', 'w', 'v']
        ... })
        >>> outlier_dfs = get_outliers_iqr(df)
        >>> outlier_dfs['A']
             A  B  C
        4  100  9  v
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    outlier_dataframes = {}
    
    # Iterate through columns and process only numeric ones
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            # Get the column data
            col_data = df[column]
            
            # Calculate quartiles and IQR
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            
            # Define outlier bounds
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Find rows where values are outliers (outside the bounds)
            outlier_mask = (col_data < lower_bound) | (col_data > upper_bound)
            
            # Get the DataFrame containing only outlier rows for this column
            outlier_rows = df[outlier_mask].copy()
            
            # Store the outlier DataFrame for this column
            outlier_dataframes[str(column)] = outlier_rows
    
    return outlier_dataframes


def remove_outliers_iqr(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with outliers in any numeric column using the IQR method.
    
    This function identifies outliers using the IQR method (values outside the range
    [Q1 - 1.5*IQR, Q3 + 1.5*IQR]) and removes any row that contains at least one
    outlier value in any numeric column.
    
    Args:
        df: pandas DataFrame to clean
        
    Returns:
        A new DataFrame with outlier rows removed. The original DataFrame is not modified.
        
    Example:
        >>> df = pd.DataFrame({
        ...     'A': [1, 2, 3, 4, 100],
        ...     'B': [5, 6, 7, 8, 9],
        ...     'C': ['x', 'y', 'z', 'w', 'v']
        ... })
        >>> cleaned_df = remove_outliers_iqr(df)
        >>> len(cleaned_df)
        4  # Row with value 100 in column A is removed
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    # Detect outliers in all numeric columns
    outliers_dict = detect_outliers_iqr(df)
    
    # Collect all unique row indices that have outliers in any column
    outlier_indices_set = set()
    for column, indices in outliers_dict.items():
        outlier_indices_set.update(indices)
    
    # Convert to sorted list for consistent output
    outlier_indices = sorted(outlier_indices_set)
    
    # Return DataFrame with outlier rows removed
    if len(outlier_indices) > 0:
        # Drop the outlier rows and return a copy
        return df.drop(index=outlier_indices).reset_index(drop=True)
    else:
        # No outliers found, return a copy of the original
        return df.copy()


def get_outlier_summary(df: pd.DataFrame, outliers_dict: Dict[str, List[int]]) -> Dict[str, Dict]:
    """Generate a summary of outliers detected in the DataFrame.
    
    Args:
        df: The original DataFrame
        outliers_dict: Dictionary returned by detect_outliers_iqr
        
    Returns:
        Dictionary with column names as keys and summary statistics as values,
        including count of outliers, percentage, and example outlier values.
    """
    summary = {}
    
    for column, indices in outliers_dict.items():
        if column not in df.columns:
            continue
            
        outlier_count = len(indices)
        total_count = len(df)
        percentage = (outlier_count / total_count * 100) if total_count > 0 else 0
        
        # Get sample outlier values (up to 5)
        sample_values = []
        if outlier_count > 0:
            sample_indices = indices[:5]
            sample_values = df.loc[sample_indices, column].tolist()
            # Convert numpy types to native Python types
            sample_values = [float(v) if pd.notna(v) else None for v in sample_values]
        
        summary[column] = {
            "outlier_count": outlier_count,
            "total_count": total_count,
            "percentage": round(percentage, 2),
            "sample_values": sample_values,
            "sample_indices": indices[:5]
        }
    
    return summary
