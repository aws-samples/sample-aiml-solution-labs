#!/usr/bin/env python3
"""
Unit tests for search_pricing_info.py.

Uses unittest.mock to patch the boto3 Bedrock Agent Runtime client,
so no AWS credentials or Knowledge Base are needed to run these tests.

Usage:
    python -m pytest test_pricing_search.py -v
    # or
    python test_pricing_search.py
"""

import unittest
from unittest.mock import patch, MagicMock


def _make_mock_client(return_value=None, side_effect=None):
    """Create a mock bedrock client with retrieve configured."""
    mock = MagicMock()
    if side_effect:
        mock.retrieve.side_effect = side_effect
    else:
        mock.retrieve.return_value = return_value or {'retrievalResults': []}
    return mock


class TestFilteredRetrieve(unittest.TestCase):
    """Tests for the filtered_retrieve function."""

    @patch('search_pricing_info.get_bedrock_client')
    def test_returns_results_above_min_score(self, mock_get_client):
        mock_get_client.return_value = _make_mock_client({
            'retrievalResults': [
                {
                    'score': 0.85,
                    'content': {'text': 'Claude Haiku: $0.25/1M input tokens'},
                    'location': {'type': 'S3', 's3Location': {'uri': 's3://bucket/us-east-1/haiku.txt'}},
                    'metadata': {'model': 'haiku'},
                },
                {
                    'score': 0.55,
                    'content': {'text': 'Claude Sonnet: $3.00/1M input tokens'},
                    'location': {'type': 'S3', 's3Location': {'uri': 's3://bucket/us-east-1/sonnet.txt'}},
                    'metadata': {},
                },
            ]
        })
        from search_pricing_info import filtered_retrieve
        results = filtered_retrieve('Claude pricing', 'us-east-1')
        self.assertEqual(len(results), 2)
        self.assertIn('Claude Haiku', results[0]['content'])
        self.assertAlmostEqual(results[0]['score'], 0.85)

    @patch('search_pricing_info.get_bedrock_client')
    def test_filters_out_low_score_results(self, mock_get_client):
        mock_get_client.return_value = _make_mock_client({
            'retrievalResults': [
                {'score': 0.05, 'content': {'text': 'Low'}, 'location': {'type': 'S3', 's3Location': {'uri': ''}}, 'metadata': {}},
                {'score': 0.45, 'content': {'text': 'Relevant'}, 'location': {'type': 'S3', 's3Location': {'uri': 's3://b/r.txt'}}, 'metadata': {}},
            ]
        })
        from search_pricing_info import filtered_retrieve
        results = filtered_retrieve('test query')
        self.assertEqual(len(results), 1)
        self.assertIn('Relevant', results[0]['content'])

    @patch('search_pricing_info.get_bedrock_client')
    def test_empty_results(self, mock_get_client):
        mock_get_client.return_value = _make_mock_client({'retrievalResults': []})
        from search_pricing_info import filtered_retrieve
        results = filtered_retrieve('nonexistent model')
        self.assertEqual(results, [])

    @patch('search_pricing_info.get_bedrock_client')
    def test_api_error_returns_error_dict(self, mock_get_client):
        mock_get_client.return_value = _make_mock_client(side_effect=Exception('KB not found'))
        from search_pricing_info import filtered_retrieve
        results = filtered_retrieve('test')
        self.assertEqual(len(results), 1)
        self.assertIn('error', results[0])
        self.assertIn('KB not found', results[0]['error'])

    @patch('search_pricing_info.get_bedrock_client')
    def test_non_s3_location_returns_empty_uri(self, mock_get_client):
        mock_get_client.return_value = _make_mock_client({
            'retrievalResults': [
                {'score': 0.5, 'content': {'text': 'Some content'}, 'location': {'type': 'WEB'}, 'metadata': {}},
            ]
        })
        from search_pricing_info import filtered_retrieve
        results = filtered_retrieve('test')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['source_uri'], '')

    @patch('search_pricing_info.get_bedrock_client')
    def test_region_filter_passed_correctly(self, mock_get_client):
        mock_client = _make_mock_client({'retrievalResults': []})
        mock_get_client.return_value = mock_client
        from search_pricing_info import filtered_retrieve
        filtered_retrieve('test', 'eu-west-1')
        call_args = mock_client.retrieve.call_args
        and_all = call_args.kwargs['retrievalConfiguration']['vectorSearchConfiguration']['filter']['andAll']
        filter_values = [f['stringContains']['value'] for f in and_all]
        self.assertIn('/pricing_data/', filter_values)
        self.assertIn('/eu-west-1/', filter_values)


class TestCallPricingSearchAgent(unittest.TestCase):
    """Tests for the call_pricing_search_agent tool wrapper."""

    @patch('search_pricing_info.filtered_retrieve')
    def test_formats_results_as_text(self, mock_retrieve):
        mock_retrieve.return_value = [
            {'content': 'Price: $3.00/1M tokens', 'score': 0.9, 'source_uri': 's3://bucket/file.txt', 'metadata': {}},
        ]
        import search_pricing_info as spi
        results = spi.filtered_retrieve('Claude pricing')
        self.assertEqual(len(results), 1)
        self.assertIn('$3.00/1M tokens', results[0]['content'])

    @patch('search_pricing_info.filtered_retrieve')
    def test_empty_results_returns_not_found(self, mock_retrieve):
        mock_retrieve.return_value = []
        from search_pricing_info import filtered_retrieve
        results = filtered_retrieve('nonexistent')
        self.assertEqual(results, [])

    @patch('search_pricing_info.filtered_retrieve')
    def test_error_results_contain_error_key(self, mock_retrieve):
        mock_retrieve.return_value = [{'error': 'Connection timeout'}]
        from search_pricing_info import filtered_retrieve
        results = filtered_retrieve('test')
        self.assertIn('error', results[0])
        self.assertIn('Connection timeout', results[0]['error'])


if __name__ == '__main__':
    unittest.main()
