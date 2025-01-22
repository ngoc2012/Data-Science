import unittest as ut
import subprocess


class TestAllClass(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.csv_files = [
            ".",
            "/home/ngoc/42/Data Science/00/samples/unreadable",
            ""
        ]
        cls.expected_outputs = [
            "No csv files found in '.'.",
            "Error: Permission denied to access the directory '/home/ngoc/42/Data Science/00/samples/unreadable'.",
            "Error: '' is not a directory."
        ]

    def test_output_matches_expected(self):
        for i, csv_file in enumerate(self.csv_files):
            result = subprocess.run(['python3', 'automatic_table.py', self.csv_files[i]], 
                                stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE, 
                                text=True)
            actual_output = result.stdout.strip()
            self.assertEqual(actual_output, self.expected_outputs[i], 
                             "Output does not match the expected output.")


if __name__ == "__main__":
    ut.main()
