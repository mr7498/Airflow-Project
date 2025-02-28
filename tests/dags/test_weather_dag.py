"""
Custom DAG Tests

This test module verifies that our custom DAGs have:
- No import errors.
- Non-empty tags.
- Default task retries set to at least two.

These tests are designed to ensure consistency and adherence to our best practices.
"""

import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag

@contextmanager
def suppress_logging(namespace):
    """
    Temporarily disable logging for a given namespace.
    """
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value

def get_import_errors():
    """
    Retrieve a list of import errors from the DagBag.
    Returns:
        List[Tuple[str, str]]: A list of tuples with relative file path and error message.
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)
    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))
    return [(None, None)] + [
        (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
    ]

def get_custom_dags():
    """
    Retrieve the custom developed DAGs from the DagBag.
    Filters for DAGs with IDs 'historical_weather_ingestion' and 'historical_weather_transformation'.
    Returns:
        List[Tuple[str, DAG, str]]: A list of tuples containing the dag_id, DAG object, and relative file location.
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)
    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))
    custom_dag_ids = {"historical_weather_ingestion", "historical_weather_transformation"}
    return [
        (dag_id, dag, strip_path_prefix(dag.fileloc))
        for dag_id, dag in dag_bag.dags.items() if dag_id in custom_dag_ids
    ]

@pytest.mark.parametrize(
    "rel_path, error_message", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, error_message):
    """
    Fail the test if any DAG file fails to import.
    """
    if rel_path and error_message:
        raise Exception(f"{rel_path} failed to import with message:\n{error_message}")

# If you want to enforce a specific set of approved tags, populate this set.
APPROVED_TAGS = set()

@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_custom_dags(), ids=[x[2] for x in get_custom_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    Ensure that each custom DAG has at least one tag.
    Optionally, check that the tags are within the approved set.
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        unapproved = set(dag.tags) - APPROVED_TAGS
        assert not unapproved, f"{dag_id} in {fileloc} contains unapproved tags: {unapproved}"

@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_custom_dags(), ids=[x[2] for x in get_custom_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    Verify that each custom DAG has a default retry count of at least 2.
    """
    retries = dag.default_args.get("retries", 0)
    assert retries >= 2, f"{dag_id} in {fileloc} must have task retries >= 2. Found: {retries}"