import pytest
from datetime import date
import polars as pl
from feat_stream.features.compute import days_since_last_late_payment, paid_loans_count, profit_in_last_90_days_rate

def make_row(loan_id, client_id, loan_status, loan_created_on, **kw):
    return {
        'loan_id': loan_id,
        'client_id': client_id,
        'loan_amount': kw.get('loan_amount', 1000.0),
        'loan_status': loan_status,
        'loan_created_on': loan_created_on,
        'loan_updated_on': loan_created_on,
        'loan_matured_on': None,
        'payment_id': kw.get('payment_id'),
        'payment_amount': None,
        'principle': None,
        'interest': kw.get('interest'),
        'payment_status': kw.get('payment_status'),
        'payment_created_on': kw.get('payment_created_on'),
        'duration': 30,
    }

def make_silver(rows):
    return pl.DataFrame(rows)

def test_paid_loans_count_only_counts_paid_status():
    silver = make_silver([
        make_row(1, 100, 'paid', date(2020, 1, 1)),
        make_row(2, 100, 'paid', date(2020, 2, 1)),
        make_row(3, 100, 'overdue', date(2020, 3, 1)),
        make_row(4, 100, 'application', date(2020, 4, 1)),
        make_row(5, 200, 'paid', date(2020, 1, 1)),
    ])
    result = paid_loans_count(silver).sort('client_id').to_dicts()
    assert result == [
        {'client_id': 100, 'paid_loans_count': 2},
        {'client_id': 200, 'paid_loans_count': 1},
    ]

def test_paid_loans_count_returns_empty_when_no_paid_loans():
    silver = make_silver([
        make_row(1, 100, 'overdue', date(2020, 1, 1)),
        make_row(2, 100, 'application', date(2020, 2, 1)),
    ])
    assert paid_loans_count(silver).is_empty()

def test_days_since_last_late_picks_max_late_date():
    today = date(2020, 12, 31)
    silver = make_silver([
        make_row(1, 100, 'paid', date(2020, 1, 1), payment_id=10,
                 payment_status='late', payment_created_on=date(2020, 6, 1)),
        make_row(2, 100, 'paid', date(2020, 2, 1), payment_id=11,
                 payment_status='late', payment_created_on=date(2020, 9, 15)),
        make_row(3, 100, 'paid', date(2020, 3, 1), payment_id=12,
                 payment_status='on_time', payment_created_on=date(2020, 11, 1)),
    ])
    result = days_since_last_late_payment(silver, today).to_dicts()
    assert result == [{'client_id': 100, 'days_since_last_late_payment': 107}]

def test_days_since_last_late_empty_when_client_never_late():
    today = date(2020, 12, 31)
    silver = make_silver([
        make_row(1, 100, 'paid', date(2020, 1, 1), payment_id=10,
                 payment_status='on_time', payment_created_on=date(2020, 6, 1)),
    ])
    assert days_since_last_late_payment(silver, today).is_empty()

def test_days_since_last_late_ignores_future_payments():
    today = date(2020, 12, 31)
    silver = make_silver([
        make_row(1, 100, 'paid', date(2020, 1, 1), payment_id=10,
                 payment_status='late', payment_created_on=date(2020, 6, 1)),
        make_row(2, 100, 'paid', date(2020, 2, 1), payment_id=11,
                 payment_status='late', payment_created_on=date(2021, 1, 1)),
    ])
    result = days_since_last_late_payment(silver, today).to_dicts()
    assert result == [{'client_id': 100, 'days_since_last_late_payment': 213}]

def test_profit_rate_ratio_for_funded_loans():
    today = date(2020, 12, 31)
    silver = make_silver([
        make_row(1, 100, 'paid', date(2020, 11, 1), loan_amount=1000.0,
                 payment_id=10, payment_status='on_time',
                 payment_created_on=date(2020, 12, 1), interest=100.0),
        make_row(2, 100, 'active', date(2020, 12, 1), loan_amount=2000.0,
                 payment_id=11, payment_status='on_time',
                 payment_created_on=date(2020, 12, 15), interest=50.0),
    ])
    result = profit_in_last_90_days_rate(silver, today).to_dicts()
    assert result == [{'client_id': 100, 'profit_in_last_90_days_rate': pytest.approx(0.05)}]

def test_profit_rate_excludes_application_status_loans():
    today = date(2020, 12, 31)
    silver = make_silver([
        make_row(1, 100, 'application', date(2020, 12, 1), loan_amount=1000.0),
    ])
    assert profit_in_last_90_days_rate(silver, today).is_empty()

def test_profit_rate_excludes_loans_outside_90_day_window():
    today = date(2020, 12, 31)
    silver = make_silver([
        make_row(1, 100, 'paid', date(2020, 1, 1), loan_amount=1000.0,
                 payment_id=10, payment_status='on_time',
                 payment_created_on=date(2020, 2, 1), interest=100.0),
    ])
    assert profit_in_last_90_days_rate(silver, today).is_empty()

def test_profit_rate_zero_when_loan_has_no_payments_yet():
    today = date(2020, 12, 31)
    silver = make_silver([
        make_row(1, 100, 'active', date(2020, 12, 15), loan_amount=1000.0),
    ])
    result = profit_in_last_90_days_rate(silver, today).to_dicts()
    assert result == [{'client_id': 100, 'profit_in_last_90_days_rate': 0.0}]

def test_profit_rate_ignores_future_payment_interest():
    today = date(2020, 12, 31)
    silver = make_silver([
        make_row(1, 100, 'active', date(2020, 12, 1), loan_amount=1000.0,
                 payment_id=10, payment_status='on_time',
                 payment_created_on=date(2021, 1, 15), interest=200.0),
    ])
    result = profit_in_last_90_days_rate(silver, today).to_dicts()
    assert result == [{'client_id': 100, 'profit_in_last_90_days_rate': 0.0}]
