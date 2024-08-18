import os
import tempfile
import unittest

import pandas as pd
import pyarrow.parquet as pq

from flen2pq.flen2pq import FixedWidthToParquetConverter


class TestFixedWidthToParquetConverter(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        # Clean up the temporary directory
        self.test_dir.cleanup()

    def test_convert_basic(self):
        input_data = ["1234567890123456", "7890123456789012"]
        expected_data = pd.DataFrame({
            'col1': [123456, 789012],
            'col2': [789012, 345678]
        })

        input_file_path = os.path.join(self.test_dir.name, 'input.txt')
        output_file_path = os.path.join(self.test_dir.name, 'output.parquet')

        with open(input_file_path, 'w') as f:
            for item in input_data:
                f.write("%s\n" % item)

        fields = [
            {'name': 'col1', 'length': 6, 'type': 'int'},
            {'name': 'col2', 'length': 6, 'type': 'int'},
        ]

        converter = FixedWidthToParquetConverter(input_file_path, output_file_path, fields)
        converter.convert()

        # Check if output Parquet file is created
        self.assertTrue(os.path.exists(output_file_path))

        # Read the Parquet file and compare with the expected data
        result_data = pd.read_parquet(output_file_path)
        pd.testing.assert_frame_equal(result_data, expected_data)

    def test_convert_monetary_type(self):
        input_data = ["123450123456", "678900789012"]
        expected_data = pd.DataFrame({
            'col1': [1234.50, 6789.00],
            'col2': [1234.56, 7890.12]
        })

        input_file_path = os.path.join(self.test_dir.name, 'input.txt')
        output_file_path = os.path.join(self.test_dir.name, 'output.parquet')

        with open(input_file_path, 'w') as f:
            for item in input_data:
                f.write("%s\n" % item)

        fields = [
            {'name': 'col1', 'length': 6, 'type': 'fixed_monetary'},
            {'name': 'col2', 'length': 6, 'type': 'fixed_monetary'},
        ]

        converter = FixedWidthToParquetConverter(input_file_path, output_file_path, fields)
        converter.convert()

        # Check if output Parquet file is created
        self.assertTrue(os.path.exists(output_file_path))

        # Read the Parquet file and compare with the expected data
        table = pq.read_table(output_file_path)
        result_data = table.to_pandas()

        pd.testing.assert_frame_equal(result_data, expected_data)

    def test_empty_input_file(self):
        input_file_path = os.path.join(self.test_dir.name, 'empty_input.txt')
        output_file_path = os.path.join(self.test_dir.name, 'output.parquet')

        with open(input_file_path, 'w') as f:
            pass  # Write an empty file

        fields = [
            {'name': 'col1', 'length': 6, 'type': 'int'},
            {'name': 'col2', 'length': 6, 'type': 'int'},
        ]

        converter = FixedWidthToParquetConverter(input_file_path, output_file_path, fields)
        converter.convert()

        # Check if the Parquet file was not created since there was no data
        self.assertFalse(os.path.exists(output_file_path))

    def test_conversion_error_handling(self):
        input_data = "abcxyz"  # Invalid data for integer conversion

        input_file_path = os.path.join(self.test_dir.name, 'input.txt')
        output_file_path = os.path.join(self.test_dir.name, 'output.parquet')

        with open(input_file_path, 'w') as f:
            f.write(input_data)

        fields = [
            {'name': 'col1', 'length': 6, 'type': 'int'},
        ]

        converter = FixedWidthToParquetConverter(input_file_path, output_file_path, fields)

        with self.assertRaises(SystemExit):  # Expect the program to exit due to an error
            converter.convert()

        # Ensure the output file was attempted to be removed
        self.assertFalse(os.path.exists(output_file_path))


if __name__ == '__main__':
    unittest.main()
