#!/bin/bash

uvicorn main:app --port 9000 --host localhost --env-file .env