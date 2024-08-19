import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
import argparse
import sys

from pandas.errors import ParserError


class TypeHandlers:

    @staticmethod
    def monetary_type_handler(str_amount: str) -> float:
        integer_part = str_amount[:-2]
        decimal_part = str_amount[-2:]
        return float(integer_part + '.' + decimal_part)

    @staticmethod
    def int_type_handler(str_int: str) -> int:
        return int(str_int.strip())


class FixedWidthToParquetConverter:
    def __init__(self, input_file, output_file, fields, chunk_size=10000, fault_tolerant = True):
        self.input_file = input_file
        self.output_file = output_file
        self.fields = fields
        self.chunk_size = chunk_size
        self.fault_tolerant = fault_tolerant

    def convert(self):
        # Extract column names and widths
        column_names = [field['name'] for field in self.fields]
        column_widths = [field['length'] for field in self.fields]

        # Initialize the Parquet file
        first_chunk = True
        pq_writer = None
        in_error = False
        # Read the file in chunks

        with pd.read_fwf(self.input_file, widths=column_widths, names=column_names, chunksize=self.chunk_size,
                         dtype=str,
                         keep_default_na=False) as file:
            try:
                for chunk in file:
                    # If the chunk is empty, we have finished reading
                    if chunk.empty:
                        break

                    # Convert data types
                    for field in self.fields:
                        col_name = field['name']
                        col_type = field['type']
                        if col_type == 'int':
                            chunk[col_name] = chunk[col_name].apply(TypeHandlers.int_type_handler)
                        elif col_type == 'float':
                            chunk[col_name] = pd.to_numeric(chunk[col_name], errors='coerce')
                        elif col_type == 'bool':
                            chunk[col_name] = pd.to_numeric(chunk[col_name], errors='coerce').astype(bool)
                        elif col_type == 'date':
                            col_format = field.get('format')
                            if not col_format:
                                raise ValueError(f"Column format not specified for date column {col_name}")
                            chunk[col_name] = pd.to_datetime(chunk[col_name], format=col_format, errors='coerce')
                        elif col_type == 'fixed_monetary':
                            chunk[col_name] = chunk[col_name].apply(TypeHandlers.monetary_type_handler)

                    # Convert the chunk to an Arrow table
                    table = pa.Table.from_pandas(chunk)

                    # Write the chunk to the Parquet file (append if not the first chunk)
                    if first_chunk:
                        pq_writer = pq.ParquetWriter(self.output_file, table.schema)
                        first_chunk = False

                    pq_writer.write_table(table)
            except ParserError as pe:
                print(pe)
                if self.fault_tolerant is False:
                    raise pe
            except Exception as e:
                in_error = True
                print(e)

        # Finalize and close the Parquet writer
        if pq_writer:
            pq_writer.close()
        if in_error:
            os.remove(self.output_file)
            print(f"Something went wrong during the Parquet conversion. Please check the input file and try again.")
            sys.exit(1)
        else:
            print(f"Conversion complete: Parquet file saved as '{self.output_file}'")


class ConfigLoader:
    @staticmethod
    def from_yaml(config_file):
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
            input_file = config['input']
            output_file = config['output']
            fault_tolerant = config['fault_tolerant']
            fields = config['fields']
            chunk_size = config.get('chunk_size', 10000)
            return input_file, output_file, fields, chunk_size, fault_tolerant

    @staticmethod
    def from_args(args):
        input_file = args.input_file
        output_file = args.output_file
        column_names = args.column_names.split(',')
        fault_tolerant = args.fault_tolerant
        column_widths = list(map(int, args.column_widths.split(',')))
        column_types = args.column_types.split(',')
        fields = [{'name': name, 'length': length, 'type': dtype}
                  for name, length, dtype in zip(column_names, column_widths, column_types)]
        chunk_size = args.chunk_size
        return input_file, output_file, fields, chunk_size, fault_tolerant


def parse_args():
    parser = argparse.ArgumentParser(description='Convert a fixed-width file to Parquet.')

    # Explicit parameters
    parser.add_argument('--input_file', type=str, help='Path to the fixed-width input file.')
    parser.add_argument('--output_file', type=str, help='Path to the output Parquet file.')
    parser.add_argument('--column_names', type=str, help='Comma-separated column names.')
    parser.add_argument('--column_widths', type=str, help='Comma-separated column widths.')
    parser.add_argument("--fault-tolerant", type=bool, help='Fault tolerant processing')
    parser.add_argument('--column_types', type=str, help='Comma-separated column types (int, float, string).')
    parser.add_argument('--chunk_size', type=int, default=10000,
                        help='Number of rows per chunk (default: 10000).')

    # YAML file parameter
    parser.add_argument('--config_file', type=str, help='Path to the YAML configuration file.')

    return parser.parse_args()

def main():
    args = parse_args()

    if args.config_file:
        # Load parameters from the YAML file
        input_file, output_file, fields, chunk_size, fault_tolerant = ConfigLoader.from_yaml(args.config_file)
    elif args.input_file and args.output_file and args.column_names and args.column_widths and args.column_types:
        # Load parameters from command-line arguments
        input_file, output_file, fields, chunk_size, fault_tolerant = ConfigLoader.from_args(args)
    else:
        print("Error: Please specify either the YAML file or all required parameters.")
        sys.exit(1)

    # Create the converter object and start the conversion
    converter = FixedWidthToParquetConverter(input_file, output_file, fields, chunk_size, fault_tolerant)
    converter.convert()

if __name__ == '__main__':
    main()
