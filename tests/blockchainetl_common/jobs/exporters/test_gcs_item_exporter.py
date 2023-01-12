import json
from unittest.mock import call, patch

import pytest

from blockchainetl_common.jobs.exporters.gcs_item_exporter import GcsItemExporter


@pytest.fixture(scope='module', autouse=True)
def mock_google_cloud_storage():
    # For now it is easiest to mock the complete storage module to avoid
    # interacting with the API completely. We obviously do not want to upload
    # any objects, but also just initialising the Client takes a relatively
    # long time and requires providing a project id.
    with patch('blockchainetl_common.jobs.exporters.gcs_item_exporter.storage') as mock:
        yield mock


@patch('blockchainetl_common.jobs.exporters.gcs_item_exporter.GcsItemExporter.upload_block_bundle')
def test_export_items_ok(mock_upload_block_bundle):
    items = [
        {'type': 'block', 'number': 0},
        {'type': 'block', 'number': 1}
    ]

    exporter = GcsItemExporter(bucket='foo', path='bar', max_workers=2)
    exporter.export_items(items)

    calls = [
        call({
            'block': {
                'type': 'block',
                'number': 0,
            },
            'transactions': [],
            'logs': [],
            'token_transfers': [],
            'traces': [],
        }),
        call({
            'block': {
                'type': 'block',
                'number': 1,
            },
            'transactions': [],
            'logs': [],
            'token_transfers': [],
            'traces': [],
        }),
    ]
    mock_upload_block_bundle.assert_has_calls(calls)


@patch('blockchainetl_common.jobs.exporters.gcs_item_exporter.GcsItemExporter.upload_block_bundle')
def test_export_items_not_ok(mock_upload_block_bundle):
    mock_upload_block_bundle.side_effect = [
        None,
        ValueError(),
    ]

    items = [
        {'type': 'block', 'number': 0},
        {'type': 'block', 'number': 1}
    ]

    exporter = GcsItemExporter(bucket='foo', path='bar', max_workers=2)
    with pytest.raises(ValueError):
        exporter.export_items(items)


def test_upload_block_bundle_ok(mock_google_cloud_storage):
    block_bundle = {'block': {'number': 0}}

    exporter = GcsItemExporter(bucket='foo', path='bar')
    exporter.upload_block_bundle(block_bundle)

    mock_storage_client = mock_google_cloud_storage.Client()
    mock_storage_client.bucket.assert_called_once_with('foo')

    mock_bucket = mock_storage_client.bucket()
    mock_bucket.blob.assert_called_once_with('bar/0.json')

    mock_blob = mock_bucket.blob()
    mock_blob.upload_from_string.assert_called_once_with(json.dumps(block_bundle))


@pytest.mark.parametrize('block_bundle,exc,match', [
    ({}, ValueError, 'block_bundle must include the block field'),
    ({'block': {}}, ValueError, 'block_bundle must include the block.number field'),
])
def test_upload_block_bundle_not_ok(block_bundle, exc, match):
    exporter = GcsItemExporter(bucket='foo', path='bar')
    with pytest.raises(exc, match=match):
        exporter.upload_block_bundle(block_bundle)
