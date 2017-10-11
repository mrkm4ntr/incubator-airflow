# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import airflow

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from datetime import datetime

DAG_NAME = 'test_subdag_parent'
CHILE_DAG_NAME = 'test_subdag'

START_DATE = datetime(2017, 10, 11)


def subdag(index):
    task_id = '%s.%s' % (CHILE_DAG_NAME, index)
    with DAG(
            dag_id='%s.%s' % (DAG_NAME, task_id),
            schedule_interval="@daily") as dag:
        DummyOperator(
            task_id='test_subdag_task',
            start_date=START_DATE)

    return task_id, dag


with DAG(
    dag_id=DAG_NAME,
    schedule_interval="@once",
) as dag:
    start = DummyOperator(
        task_id='start',
        start_date=START_DATE)
    end = DummyOperator(
        task_id='end',
        start_date=START_DATE)
    for i in range(2):
        task_id, sub_dag = subdag(i)
        sub = SubDagOperator(
            task_id=task_id,
            subdag=sub_dag,
            start_date=START_DATE)
        start >> sub >> end
