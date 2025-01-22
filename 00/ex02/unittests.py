import unittest as ut
import subprocess


class TestAllClass(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.csv_files = [
            "data_col_error.csv",
            "data_col.csv",
            "data_head.csv",
            ""
        ]
        cls.expected_outputs = [
            "Error: '/home/ngoc/42/Data Science/00/samples/data_col_error.csv' has incorrect column.",
            "Error: '/home/ngoc/42/Data Science/00/samples/data_col.csv' must have 6 columns.",
            "Error: '/home/ngoc/42/Data Science/00/samples/data_head.csv' has incorrect column.",
            "Error: '/home/ngoc/42/Data Science/00/samples/' is not a file."
        ]

    def test_tester_output_matches_expected(self):
        for i, csv_file in enumerate(self.csv_files):
            result = subprocess.run(['python3', 'table.py', self.csv_files[i]], 
                                stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE, 
                                text=True)
            actual_output = result.stdout.strip()
            self.assertEqual(actual_output, self.expected_outputs[i], 
                             "Output does not match the expected output.")


if __name__ == "__main__":
    ut.main()
