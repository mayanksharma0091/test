SET FEEDBACK ON
SET TIMING ON
SET SERVEROUTPUT ON SIZE 1000000

WHENEVER SQLERROR CONTINUE NONE;

PROMPT 'Creating BASELOAD JOB table BASE_JOB';

BEGIN
   dbms_scheduler.create_job ( job_name        => 'BASE_JOB', 
                               job_type        => 'PLSQL_BLOCK',                               							   
							   job_action      => 'BEGIN GET_VALID_PRICE_P1(''${base.start.date}''); END;', 
							   start_date      => CURRENT_DATE, 							   
							   enabled         => TRUE,
                               comments        => 'Baseload to insert valid price records into table.');   
                               
   dbms_scheduler.set_attribute ('BASE_JOB','max_failures',3);
   
   dbms_scheduler.set_attribute ('BASE_JOB','restartable',TRUE);   
END;
/