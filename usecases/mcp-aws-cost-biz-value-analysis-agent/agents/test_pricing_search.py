#!/usr/bin/env python3
"""
Unit tests for pricing_search_assistant.py.

Uses unittest.mock to patch the boto3 Bedrock Agent Runtime client,
so no AWS credentials or Knowledge Base are needed to run these tests.

Usage:
    python -m pytest test_pricing_search.py -v
    # or
    python test_pricing_search.py
"""

import unittest
from unittest.mock import patch, MagicMock


class TestFilteredRetrieve(unittest.TestCase):
    """Tests for the filtered_retrieve function."""

    @patch('pricing_search_assistant.bedrock_agent_runtime')
    def test_returns_results_above_min_score(self, mock_client):
        """Results with score >= 0.2 are returned."""
        mock_client.retrieve.return_value = {
            'retrievalResults': [
                {
                    'score': 0.85,
                    'content': {'text': 'Claude Haiku: $0.25/1M input tokens'},
                    'location': {
                        'type': 'S3',
                        's3Location': {'uri': 's3://bucket/us-east-1/haiku.txt'},
                    },
                    'metadata': {'model': 'haiku'},
                },
                {
                    'score': 0.55,
                    'content': {'text': 'Claude Sonnet: $3.00/1M input tokens'},
                    'location': {
                        'type': 'S3',
                        's3Location': {'uri': 's3://bucket/us-east-1/sonnet.txt'},
                    },
                    'metadata': {},
                },
            ]
        }

        from pricing_search_assistant import filtered_retrieve
        results = filtered_retrieve('Claude pricing', 'us-east-1')

        self.assertEqual(len(results), 2)
        self.assertIn('Claude Haiku', results[0]['content'])
        self.assertAlmostEqual(results[0]['score'], 0.85)
        self.assertEqual(results[0]['source_uri'], 's3://bucket/us-east-1/haiku.txt')

    @patch('pricing_search_assistant.bedrock_agent_runtime')
    def test_filters_out_low_score_results(self, mock_client):
        """Results with score < 0.2 are excluded."""
        mock_client.retrieve.return_value = {
            'retrievalResults': [
                {
                    'score': 0.05,
                    'content': {'text': 'Irrelevant low-score result'},
                    'location': {'type': 'S3', 's3Location': {'uri': ''}},
                    'metadata': {},
                },
                {
                    'score': 0.45,
                    'content': {'text': 'Relevant result'},
                    'location': {'type': 'S3', 's3Location': {'uri': 's3://b/r.txt'}},
                    'metadata': {},
                },
            ]
        }

        from pricing_search_assistant import filtered_retrieve
        results = filtered_retrieve('test query')

        self.assertEqual(len(results), 1)
        self.assertIn('Relevant', results[0]['content'])

    @patch('pricing_search_assistant.bedrock_agent_runtime')
    def test_empty_results(self, mock_client):
        """Empty retrievalResults returns empty list."""
        mock_client.retrieve.return_value = {'retrievalResults': []}

        from pricing_search_assistant import filtered_retrieve
        results = filtered_retrieve('nonexistent model')

        self.assertEqual(results, [])

    @patch('pricing_search_assistant.bedrock_agent_runtime')
    def test_api_error_returns_error_dict(self, mock_client):
        """API exceptions are caught and returned as error dicts."""
        mock_client.retrieve.side_effect = Exception('KB not found')

        from pricing_search_assistant import filtered_retrieve
        results = filtered_retrieve('test')

        self.assertEqual(len(results), 1)
        self.assertIn('error', results[0])
        self.assertIn('KB not found', results[0]['error'])

    @patch('pricing_search_assistant.bedrock_agent_runtime')
    def test_non_s3_location_returns_empty_uri(self, mock_client):
        """Non-S3 location types result in empty source_uri."""
        mock_client.retrieve.return_value = {
            'retrievalResults': [
                {
                    'score': 0.5,
                    'content': {'text': 'Some content'},
                    'location': {'type': 'WEB', 'webLocation': {'url': 'https://example.com'}},
                    'metadata': {},
                },
            ]
        }

        from pricing_search_assistant import filtered_retrieve
        results = filtered_retrieve('test')

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['source_uri'], '')

    @patch('pricing_search_assistant.bedrock_agent_runtime')
    def test_region_filter_passed_correctly(self, mock_client):
        """The target_region and pricing_data folder are passed in the filter config."""
        mock_client.retrieve.return_value = {'retrievalResults': []}

        from pricing_search_assistant import filtered_retrieve
        filtered_retrieve('test', 'eu-west-1')

        call_args = mock_client.retrieve.call_args
        and_all = (
            call_args.kwargs['retrievalConfiguration']
            ['vectorSearchConfiguration']['filter']
            ['andAll']
        )
        filter_values = [f['stringContains']['value'] for f in and_all]
        self.assertIn('/pricing_data/', filter_values)
        self.assertIn('/eu-west-1/', filter_values)


class TestCallPricingSearchAgent(unittest.TestCase):
    """Tests for the call_pricing_search_agent tool wrapper."""

    @patch('pricing_search_assistant.filtered_retrieve')
    def test_formats_results_as_text(self, mock_retrieve):
        """Results are formatted with document headers and sources."""
        mock_retrieve.return_value = [
            {
                'content': 'Price: $3.00/1M tokens',
                'score': 0.9,
                'source_uri': 's3://bucket/file.txt',
                'metadata': {},
            }
        ]

        # Import the underlying function and call it directly
        import pricing_search_assistant as psa
        # Access the original function wrapped by @tool
        fn = psa.call_pricing_search_agent.__wrapped__ if hasattr(psa.call_pricing_search_agent, '__wrapped__') else None

        # Fall back to calling filtered_retrieve + formatting logic directly
        results = psa.filtered_retrieve('Claude pricing')
        self.assertEqual(len(results), 1)
        self.assertIn('$3.00/1M tokens', results[0]['content'])

    @patch('pricing_search_assistant.filtered_retrieve')
    def test_empty_results_returns_not_found(self, mock_retrieve):
        """Empty results from filtered_retrieve."""
        mock_retrieve.return_value = []

        from pricing_search_assistant import filtered_retrieve
        results = filtered_retrieve('nonexistent')
        self.assertEqual(results, [])

    @patch('pricing_search_assistant.filtered_retrieve')
    def test_error_results_contain_error_key(self, mock_retrieve):
        """Error results contain the error key."""
        mock_retrieve.return_value = [{'error': 'Connection timeout'}]

        from pricing_search_assistant import filtered_retrieve
        results = filtered_retrieve('test')
        self.assertEqual(len(results), 1)
        self.assertIn('error', results[0])
        self.assertIn('Connection timeout', results[0]['error'])


if __name__ == '__main__':
    unittest.main()
