#!/bin/bash
set -e
pg_restore -U postgres -d loans --no-owner --no-acl /docker-entrypoint-initdb.d/de_test_task_db