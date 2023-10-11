import unittest
from string_processing import test_keys_values, test_locations

class TestStringProcessing(unittest.TestCase):

    def test_test_keys_values_valid_dict(self):
        valid_dict = {"key1": "value1", "key2": ["value2"]}
        with self.assertRaises(ValueError):
            test_keys_values(valid_dict)

    def test_test_keys_values_invalid_dict(self):
        invalid_dict = "not_a_dict"
        with self.assertRaises(ValueError):
            test_keys_values(invalid_dict)

        # Add more test cases as needed

    def test_test_locations_valid_list(self):
        valid_locations = [{"studio": "Studio1", "city": "City1", "country": "Country1"},
                           {"studio": "Studio2", "city": "City2", "country": "Country2"}]
        with self.assertRaises(ValueError):
            test_locations(valid_locations)

    def test_test_locations_invalid_list(self):
        invalid_list = "not_a_list"
        with self.assertRaises(ValueError):
            test_locations(invalid_list)

        # Add more test cases as needed

# Run the tests
if __name__ == '__main__':
    unittest.main()
