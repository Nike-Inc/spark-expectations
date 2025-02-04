import pytest
from unittest.mock import MagicMock
import pandas as pd

from spark_expectations.utils.trial import DataProcessor


# Fixture to mock the context object
@pytest.fixture
def mock_context():
    context = MagicMock()
    # Mock the return values for get_detailed_dataframe and get_detailed_dataframe1
    context.get_detailed_dataframe.return_value = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    context.get_detailed_dataframe1.return_value = pd.DataFrame({'col1': [5, 6], 'col2': [7, 8]})
    return context

# Fixture to create an instance of DataProcessor with the mocked context
@pytest.fixture
def data_processor(mock_context):
    return DataProcessor()

# Test case for the dataload function
def test_dataload(data_processor):
    # Call the dataload method
    result = data_processor.dataload()

    # Expected result after adding the two DataFrames
    # expected_result = pd.DataFrame({'col1': [6, 8], 'col2': [10, 12]})
    #
    # # Assert that the result matches the expected result
    # pd.testing.assert_frame_equal(result, expected_result)
    #
    # # Verify that the mocked methods were called
    # data_processor.context.get_detailed_dataframe.assert_called_once()
    # data_processor.context.get_detailed_dataframe1.assert_called_once()