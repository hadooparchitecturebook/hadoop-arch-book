#!/bin/bash
psql -d oltp -U movielens -f create_tables.sql
