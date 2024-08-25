from hamutils.adif.adi import ADIReader, ADIWriter

import argparse
import functools
import sys


class ADIFBatchEdit:
    @staticmethod
    def _open_adif_to_write(output_file):
        return ADIWriter(output_file, "adif_batch_edit", "0.1", compact=True)

    @staticmethod
    def _add_field(fieldname, value, record):
        record.setdefault(fieldname.lower(), value)

    @staticmethod
    def _update_field(fieldname, value, record):
        record[fieldname.lower()] = value

    @staticmethod
    def _delete_field(fieldname, record):
        record.pop(fieldname.lower())

    @staticmethod
    def _keep_fields(fields, record):
        for key in set(record.keys()).difference(fields):
            record.pop(key)

    def __init__(self, input_file, output_file):
        self._input_file = input_file
        self._output_file = output_file
        self._ops = []

    def add_operations(self, op_string, sep="|"):
        op, *fields = op_string.split(sep)

        match op.lower():
            case "add":
                for field in fields:
                    fieldname, value = field.split("=", 1)
                    self._ops.append(functools.partial(ADIFBatchEdit._add_field, fieldname, value))
            case "update":
                for field in fields:
                    fieldname, value = field.split("=", 1)
                    self._ops.append(functools.partial(ADIFBatchEdit._update_field, fieldname, value))
            case "delete":
                for field in fields:
                    self._ops.append(functools.partial(ADIFBatchEdit._delete_field, field))
            case "keep":
                self._ops.append(functools.partial(ADIFBatchEdit._keep_fields, fields))
            case _:
                raise RuntimeError(f"Invalid operation: {op_string}")

    def run_batch(self):
        with self._input_file as input_file, self._output_file as output_file:
            reader = ADIReader(input_file)
            writer = self._open_adif_to_write(self._output_file)
            writer.write_header()
            for record in reader:
                for op in self._ops:
                    op(record)
                writer.add_qso(**record)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--input-file", type=argparse.FileType("r"), default=sys.stdin)
    parser.add_argument("-o", "--output-file", type=argparse.FileType("wb"), default=sys.stdout.buffer)
    parser.add_argument("--seperator", default="|")

    parser.add_argument("operations", nargs="+")

    args = parser.parse_args()

    editor = ADIFBatchEdit(args.input_file, args.output_file)

    for operation in args.operations:
        editor.add_operations(operation, args.seperator)

    editor.run_batch()
