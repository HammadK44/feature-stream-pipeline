import pytest
from datetime import date
from feat_stream.db.postgres import connect
from feat_stream.state import bookmarks

@pytest.fixture(autouse=True)
def clean_bookmarks():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM pipeline_bookmarks')
        conn.commit()
    yield
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM pipeline_bookmarks')
        conn.commit()

def test_read_returns_default_when_no_row_exists():
    result = bookmarks.read('user')
    assert result == bookmarks.DEFAULT_BOOKMARK

def test_update_inserts_when_no_row_exists():
    bookmarks.update('user', date(2020, 5, 1))
    assert bookmarks.read('user') == date(2020, 5, 1)

def test_update_overwrites_existing_row():
    bookmarks.update('user', date(2020, 5, 1))
    bookmarks.update('user', date(2020, 6, 1))
    assert bookmarks.read('user') == date(2020, 6, 1)

def test_bookmarks_are_per_table():
    bookmarks.update('user', date(2020, 5, 1))
    bookmarks.update('loan', date(2020, 6, 1))
    assert bookmarks.read('user') == date(2020, 5, 1)
    assert bookmarks.read('loan') == date(2020, 6, 1)
