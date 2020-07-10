from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import DataQualityOperator
from airflow.operators import ProcessDimensionsOperator
from airflow.operators import ProcessImmigrantOperator
from airflow.operators import CapstoneDataQualityOperator
from airflow.models import Variable



EXEC_DATE = "{{ ds }}"

save_path = Variable.get("save_path")
path_load = Variable.get("load_path")

#log.info('Processing capstone data')


default_args = {
    'owner': 'Mukunda',
    'start_date': datetime(2016, 1, 1),
    'provide_context': True,
    'end_date': datetime(2016, 2, 1)
    
}



dag = DAG('USA_immigration_data_pipeline',
          default_args=default_args,
          description='Load and transform data for capstone with Airflow',
          schedule_interval= '@monthly',
          catchup = True
          )





start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


proces_dimensions_operator = ProcessDimensionsOperator(
                            task_id = "load_dimension_data",
                            dag = dag,
                            data_dir = path_load,
                            save_path = save_path,
                            mode = "delete"
                            )

process_fact_operator = ProcessImmigrantOperator(
                        task_id = "load_immigration_fact_data",
                        dag = dag,
                        load_immigrant_data_path = path_load,
                        save_path = save_path,
                        exec_date = EXEC_DATE,
                        provide_context = True
                        )

check_data = CapstoneDataQualityOperator(
                            task_id = "check_data_quality",
                            dag = dag,
                            raw_data_path = path_load,
                            warehouse_path = save_path,
                            exec_date = EXEC_DATE
                            )


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> proces_dimensions_operator >>  process_fact_operator >> check_data >> end_operator
