from ngi_pipeline.utils.pyutils import flatten

def test_flatten():
    nested_list = [['A', 'B'], ['C', 'D'], 'E']
    expected_list = ['A', 'B', 'C', 'D', 'E']
    flattened_list = flatten(nested_list)
    for got_element, expected_element in zip(flattened_list, expected_list):
        assert got_element == expected_element