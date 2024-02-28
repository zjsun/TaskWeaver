#!/bin/bash

work_dir="$(dirname $0)/../playground/UI/"
cd $work_dir && chainlit run app.py
