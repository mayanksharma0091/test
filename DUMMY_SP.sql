CREATE OR REPLACE PACKAGE BODY DH_BUS_TRANS.POSLOG_SAPAY_INT_SP AS
/*******************************************************************************
DESCRIPTION: Package POSLOG_SAPAY_INT_SP created for loading SA.Payment receipt data into respective tables.
             OSB LIP integration will invoke package procedure POSLOG_INTEGRATION_P1.

REQUEST #38292

HISTORY
---------
DATE               NAME                    REMARK
-------------      ----------------        -------------------------------------
2018-07-19         ASIM KHAN PATHAN        CREATOR
*******************************************************************************/
FUNCTION VALIDATE_COL_VALUE_F1(P_COL_VALUE VARCHAR2, P_DATATYPE VARCHAR2, P_SIZE VARCHAR2, P_REMARK VARCHAR2)
RETURN BOOLEAN
/*******************************************************************************
DESCRIPTION: FUNCTION VALIDATE_COL_VALUE_F1 IS RESPONSIBLE TO VALIDATE THE GIVEN VALUE WITH GIVEN DATATYPE.

REQUEST #38292

HISTORY
---------
DATE               NAME                    REMARK
-------------      ----------------        -------------------------------------
2018-07-19         ASIM KHAN PATHAN        CREATOR
*******************************************************************************/
IS
  V_PRECISION NUMBER;
  V_SCALE     NUMBER;
  V_RETURN    BOOLEAN; --VARCHAR2(1000);
  V_DATE      DATE;
BEGIN
  IF PV_DEBUG_LEVEL = 2
  THEN
    PL_INT_LOG_P1(P_LINE_IND => 'N', P_TABLE_NAME => 'VALIDATE_COL_VALUE_F1', P_ERROR_MESSAGE => 'VALIDATE_COL_VALUE_F1# '||P_REMARK||P_COL_VALUE||', P_DATATYPE ='||P_DATATYPE||', P_SIZE ='||P_SIZE , P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
    PV_STEP_SEQ := PV_STEP_SEQ + 1;
  END IF;
  IF P_COL_VALUE IS NULL
  THEN
     V_RETURN := (1=1);
     RETURN V_RETURN;
  END IF;
  IF P_DATATYPE = 'N'
  THEN
    IF REGEXP_LIKE(P_COL_VALUE, '^-?[[:digit:],.]*$')
    THEN
      /*  IF P_SIZE <> '0'
        THEN
          V_PRECISION := TO_NUMBER(SUBSTR(P_SIZE, 1, INSTR(P_SIZE, ',') - 1));
          V_SCALE := TO_NUMBER(SUBSTR(P_SIZE, INSTR(P_SIZE, ',') + 1));
         -- DBMS_OUTPUT.PUT_LINE('V_PRECISION:'||V_PRECISION||' ;V_SCALE:'||V_SCALE||' ;:ABC'||LENGTH(TRANSLATE(P_COL_VALUE,'1234567890.-','1234567890'))||' ;CBD:'||LENGTH(SUBSTR(P_COL_VALUE, 1, INSTR(P_COL_VALUE, '.') - 1))||' ;XYZ:'||LENGTH(SUBSTR(P_COL_VALUE, INSTR(P_COL_VALUE, '.') + 1)));
          IF INSTR(P_COL_VALUE, '.') > 0 AND V_SCALE > 0
          THEN
              V_RETURN := (LENGTH(TRANSLATE(P_COL_VALUE,'1234567890.-','1234567890')) <= V_PRECISION
                            AND LENGTH(SUBSTR(P_COL_VALUE, 1, INSTR(P_COL_VALUE, '.') - 1)) <= (V_PRECISION - V_SCALE)
                            AND LENGTH(SUBSTR(P_COL_VALUE, INSTR(P_COL_VALUE, '.') + 1)) <= V_SCALE);
          ELSIF  INSTR(P_COL_VALUE, '.') > 0 AND V_SCALE = 0
          THEN
             V_RETURN := (LENGTH(SUBSTR(P_COL_VALUE, 1, INSTR(P_COL_VALUE, '.') - 1)) <= V_PRECISION);
          ELSE
             V_RETURN := (LENGTH(P_COL_VALUE) <= V_PRECISION);
          END IF;
        ELSE  */
          V_RETURN := (1=1);
      --  END IF;
    ELSE
        V_RETURN := (1=2);
    END IF;
  ELSIF P_DATATYPE = 'D'
  THEN
      BEGIN
        V_DATE := TO_DATE(SUBSTR(TO_CHAR(REPLACE(P_COL_VALUE, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS');
        V_RETURN := (1=1);
      EXCEPTION
        WHEN OTHERS THEN
          V_RETURN := (1=2);
      END;
  END IF;
  RETURN V_RETURN;
END VALIDATE_COL_VALUE_F1;
--==============================================================================
PROCEDURE PL_INT_LOG_P1(P_LINE_IND VARCHAR2, P_TABLE_NAME VARCHAR2, P_ERROR_MESSAGE VARCHAR2, P_STEP_SEQ_NO NUMBER, P_DEBUG_LEVEL NUMBER)
is
PRAGMA AUTONOMOUS_TRANSACTION ;
/*******************************************************************************
Description: Procedure PL_INT_LOG_P1 is responsible to the execution and errors into log tagle CEM_PL_INT_LOG_T.

Request #38292

History
---------
Date               Name                    Remark
-------------      ----------------        -------------------------------------
2018-07-19         Asim Khan Pathan        Creator
*******************************************************************************/
begin

  INSERT INTO DH_BUS_TRANS.CEM_PL_INT_LOG_T
  (TRA_SEQ_NO,
    TRA_START_TIME,
    WORKSTATION_ID,
    BU_CODE,
    BU_TYPE,
    LINE_IND,
    TABLE_NAME,
    REMARK_MESSAGE,
    SAPP_INSERT_DATE,
    STEP_SEQ_NO,
    DEBUG_LEVEL)
    values
    (PV_TRA_SEQ_NO,
    PV_TRA_START_TIME,
    PV_WORKSTATION_ID,
    PV_BU_CODE,
    PV_BU_TYPE,
    P_LINE_IND,
    P_TABLE_NAME,
    P_ERROR_MESSAGE,
    SYSDATE,
    P_STEP_SEQ_NO,
    P_DEBUG_LEVEL
    );

  --Delete log older than last 30 days.
    if (to_number(to_char(sysdate, 'hh24')) in (1, 13))
    and (EXTRACT(MINUTE FROM SYSTIMESTAMP) between 1 and 3)
    and P_LINE_IND = 'N'
    then
      DELETE FROM CEM_PL_INT_LOG_T WHERE TRUNC(SAPP_INSERT_DATE) < TRUNC(SYSDATE) - 30;
    end if;

    commit;
exception
  when others then
     rollback;
end PL_INT_LOG_P1;

--==============================================================================
PROCEDURE POSLOG_INTEGRATION_P1(P_TRANSACTION_TAB IN DH_BUS_TRANS.TRANSACTION_TAB, P_RESPONSE OUT VARCHAR2) IS
PRAGMA AUTONOMOUS_TRANSACTION ;
/*******************************************************************************
Description: Procedure POSLOG_INTEGRATION_P1 is responsible to insert POSLOG data into
             local SAPP database(using the same data model as the central datacache).

Request #38292

History
---------
Date               Name                    Remark
-------------      ----------------        -------------------------------------
2018-07-10         Asim Khan Pathan        Creator
*******************************************************************************/
    V_SAPP_INSERT_DATE          DATE;
    V_SAPP_UPDATE_DATE          DATE;
    V_COUNT                     NUMBER := 0;
    V_ERR_MSG                   VARCHAR2(4000) := null;
    V_SUCCESS                   VARCHAR2(1):='Y';
    V_TRA_ITEM_TAB              DH_BUS_TRANS.TRA_ITEM_TAB;
    V_TRA_CUSTOMER_TAB          DH_BUS_TRANS.TRA_CUSTOMER_TAB;
    V_TRA_DISC_TAB              DH_BUS_TRANS.TRA_DISC_TAB;
    V_TRA_GIFT_CERT_TAB         DH_BUS_TRANS.TRA_GIFT_CERT_TAB;
    V_TRA_LOYALTY_RW_TAB        DH_BUS_TRANS.TRA_LOYALTY_RW_TAB;
    V_TRA_SIGNATURE_TAB         DH_BUS_TRANS.TRA_SIGNATURE_TAB;
    V_TRA_SURVEY_TAB            DH_BUS_TRANS.TRA_SURVEY_TAB;
    V_TRA_TAX_TAB               DH_BUS_TRANS.TRA_TAX_TAB;
    V_TRA_TOTAL_TAB             DH_BUS_TRANS.TRA_TOTAL_TAB;
    V_TRA_TENDER_TAB            DH_BUS_TRANS.TRA_TENDER_TAB;
    V_TRA_LINE_DISC_TAB         DH_BUS_TRANS.TRA_LINE_DISC_TAB;
    V_TRA_LINE_TAX_TAB          DH_BUS_TRANS.TRA_LINE_TAX_TAB;
    V_TRA_LINE_SUBITEM_TAB      DH_BUS_TRANS.TRA_LINE_SUBITEM_TAB;
    V_TRA_LINE_SUBITEM_TAX_TAB  DH_BUS_TRANS.TRA_LINE_SUBITEM_TAX_TAB;
    V_datatype_err              varchar2(35) := 'wrong datatype format for column: ';
    V_consolidated_err_smg      varchar2(4000);
    V_TRA_SEQ_NO                CEM_PL_TRANSACTION_T.TRA_SEQ_NO%TYPE;
    V_TRA_START_TIME            CEM_PL_TRANSACTION_T.TRA_START_TIME%TYPE;
    V_WORKSTATION_ID            CEM_PL_TRANSACTION_T.WORKSTATION_ID%TYPE;
    V_BU_CODE                   CEM_PL_TRANSACTION_T.BU_CODE%TYPE;
    V_BU_TYPE                   CEM_PL_TRANSACTION_T.BU_TYPE%TYPE;
    V_TAB_NAME                  VARCHAR2(30);
    PV_STEP_SEQ                  NUMBER:=0;
    V_SESSION_INFO              VARCHAR2(500);
BEGIN
    --///Initiating collection
    V_TRA_ITEM_TAB              := DH_BUS_TRANS.TRA_ITEM_TAB();
    V_TRA_CUSTOMER_TAB          := DH_BUS_TRANS.TRA_CUSTOMER_TAB();
    V_TRA_DISC_TAB              := DH_BUS_TRANS.TRA_DISC_TAB();
    V_TRA_GIFT_CERT_TAB         := DH_BUS_TRANS.TRA_GIFT_CERT_TAB();
    V_TRA_LOYALTY_RW_TAB        := DH_BUS_TRANS.TRA_LOYALTY_RW_TAB();
    V_TRA_SIGNATURE_TAB         := DH_BUS_TRANS.TRA_SIGNATURE_TAB();
    V_TRA_SURVEY_TAB            := DH_BUS_TRANS.TRA_SURVEY_TAB();
    V_TRA_TAX_TAB               := DH_BUS_TRANS.TRA_TAX_TAB();
    V_TRA_TOTAL_TAB             := DH_BUS_TRANS.TRA_TOTAL_TAB();
    V_TRA_TENDER_TAB            := DH_BUS_TRANS.TRA_TENDER_TAB();
    V_TRA_LINE_DISC_TAB         := DH_BUS_TRANS.TRA_LINE_DISC_TAB();
    V_TRA_LINE_TAX_TAB          := DH_BUS_TRANS.TRA_LINE_TAX_TAB();
    V_TRA_LINE_SUBITEM_TAB      := DH_BUS_TRANS.TRA_LINE_SUBITEM_TAB();
    V_TRA_LINE_SUBITEM_TAX_TAB  := DH_BUS_TRANS.TRA_LINE_SUBITEM_TAX_TAB();
    ----
    --////Receipt loading started
    IF P_TRANSACTION_TAB IS NOT NULL
    THEN
        IF PV_DEBUG_LEVEL IN (1,2)
        THEN
              SELECT 'Transaction loading started from IP_ADDRESS: '||SYS_CONTEXT('USERENV','IP_ADDRESS')  INTO V_SESSION_INFO  FROM DUAL;
        ELSIF PV_DEBUG_LEVEL = 3
        THEN
              SELECT 'Transaction loading started from IP_ADDRESS: '||'IP ADDRESS: '||SYS_CONTEXT('USERENV','IP_ADDRESS')||' / OS USER (CLIENT): '||SYS_CONTEXT('USERENV','OS_USER')||' / CALLING PROGRAM: '||SYS_CONTEXT('USERENV','MODULE')   INTO V_SESSION_INFO  FROM DUAL;
        END IF;
        -- Loading CEM_PL_TRANSACTION_T
        V_TAB_NAME := 'CEM_PL_TRANSACTION_T';
        FOR I IN  1..P_TRANSACTION_TAB.COUNT
        LOOP
           BEGIN
               PV_TRA_SEQ_NO 		  :=  P_TRANSACTION_TAB(i).TRA_SEQ_NO;
               PV_TRA_START_TIME 	:=  P_TRANSACTION_TAB(i).TRA_START_TIME;
               PV_WORKSTATION_ID 	:=  P_TRANSACTION_TAB(i).WORKSTATION_ID;
               PV_BU_CODE 			  :=  P_TRANSACTION_TAB(i).BU_CODE;
               PV_BU_TYPE 			  :=  P_TRANSACTION_TAB(i).BU_TYPE;
               V_BU_CODE          :=  SUBSTR(P_TRANSACTION_TAB(i).BU_CODE, 1, 5);
               V_BU_TYPE          :=  case when UPPER(P_TRANSACTION_TAB(i).BU_TYPE)='RETAILSTORE' then 'STO' else 'STO' end;
               V_TRA_SEQ_NO       :=  to_number(P_TRANSACTION_TAB(i).TRA_SEQ_NO);
               V_TRA_START_TIME   :=  to_timestamp(substr(to_char(replace(P_TRANSACTION_TAB(i).TRA_START_TIME, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS');
               V_WORKSTATION_ID   :=  to_number(P_TRANSACTION_TAB(i).WORKSTATION_ID);
               ---------
               PL_INT_LOG_P1(P_LINE_IND => 'N', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_SESSION_INFO, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
               PV_STEP_SEQ := PV_STEP_SEQ + 1;
               ---
               --Validate number and date datatype of the given value
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).TRA_START_TIME, p_datatype => 'D', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_START_TIME'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).TRA_END_TIME, p_datatype => 'D', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_END_TIME'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_END_TIME', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).WORKSTATION_ID, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.WORKSTATION_ID'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'WORKSTATION_ID', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).OPERATOR_ID, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.OPERATOR_ID'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'OPERATOR_ID', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).TRANSLINK_TRA_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRANSLINK_TRA_SEQ_NO'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'TRANSLINK_TRA_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).TRANSLINK_WORKSTATION_ID, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRANSLINK_WORKSTATION_ID'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'TRANSLINK_WORKSTATION_ID', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).BUSINESS_DAY, p_datatype => 'D', p_size => '0', P_REMARK => V_TAB_NAME||'.BUSINESS_DAY'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'BUSINESS_DAY', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).TRANSLINK_BUSINESS_DAY, p_datatype => 'D', p_size => '0', P_REMARK => V_TAB_NAME||'.TRANSLINK_BUSINESS_DAY'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'TRANSLINK_BUSINESS_DAY', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).TRANSLINK_TRA_START_TIME, p_datatype => 'D', p_size => '0', P_REMARK => V_TAB_NAME||'.TRANSLINK_TRA_START_TIME'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'TRANSLINK_TRA_START_TIME', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               if not(validate_col_value_f1(p_col_value => P_TRANSACTION_TAB(i).TRA_END_TIME, p_datatype => 'D', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_END_TIME'||': ' ))
               then
                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRANSACTION_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_END_TIME', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
               end if;
               -------
               INSERT INTO DH_BUS_TRANS.CEM_PL_TRANSACTION_T
                 (TRA_SEQ_NO
                  ,TRA_START_TIME
                  ,WORKSTATION_ID
                  ,BU_CODE
                  ,BU_TYPE
                  ,TRANSLINK_BU_CODE
                  ,TRANSLINK_BU_TYPE
                  ,TRANSLINK_REASON_CODE
                  ,TRA_STATUS
                  ,CUSTOMER_INVOICE
                  ,EXTERNAL_INVOICE
                  ,RECEIPT_IMAGE
                  ,BUSINESS_DAY
                  ,TRANSLINK_BUSINESS_DAY
                  ,OPERATOR_ID
                  ,TRANSLINK_TRA_SEQ_NO
                  ,TRANSLINK_WORKSTATION_ID
                  ,TRANSLINK_TRA_START_TIME
                  ,TRA_END_TIME
                  ,BARCODE_NUMBER
                  ,CANCEL_FLAG
                  ,CUR_CODE
                  ,CUSTOMER_GROUP
                  ,DIVISION_CODE
                  ,HOME_DELIVERY_FLAG
                  ,LOYALTY_ACCOUNT_ID
                  ,LOYALTY_PROGRAM_ID
                  ,OFFLINE_FLAG
                  ,OPERATOR_NAME
                  ,OPERATOR_TYPE
                  ,ORGANISATION_HIERARCHY
                  ,ORGANISATION_HIERARCHY_ID
                  ,ORGANISATION_HIERARCHY_LEVEL
                  ,PRICE_COMMUNICATION_AREA
                  ,TAX_REQUEST_RESULT
                  ,TAX_REQUEST_TYPE
                  ,TILL_ID
                  ,SAPP_INSERT_DATE
                  ,SAPP_UPDATE_DATE
                  )
                  VALUES
                  (V_TRA_SEQ_NO
                  ,V_TRA_START_TIME
                  ,V_WORKSTATION_ID
                  ,V_BU_CODE
                  ,v_BU_TYPE
                  ,SUBSTR(P_TRANSACTION_TAB(i).TRANSLINK_BU_CODE, 1, 5)
                  ,case when UPPER(P_TRANSACTION_TAB(i).TRANSLINK_BU_TYPE)='RETAILSTORE' then 'STO' else 'STO' end
                  ,SUBSTR(P_TRANSACTION_TAB(i).TRANSLINK_REASON_CODE, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).TRA_STATUS, 1, 30)
                  ,P_TRANSACTION_TAB(i).CUSTOMER_INVOICE
                  ,P_TRANSACTION_TAB(i).EXTERNAL_INVOICE
                  ,P_TRANSACTION_TAB(i).RECEIPT_IMAGE
                  ,to_timestamp(substr(to_char(replace(P_TRANSACTION_TAB(i).BUSINESS_DAY, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                  ,to_timestamp(substr(to_char(replace(P_TRANSACTION_TAB(i).TRANSLINK_BUSINESS_DAY, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                  ,to_number(P_TRANSACTION_TAB(i).OPERATOR_ID)
                  ,to_number(P_TRANSACTION_TAB(i).TRANSLINK_TRA_SEQ_NO)
                  ,to_number(P_TRANSACTION_TAB(i).TRANSLINK_WORKSTATION_ID)
                  ,to_timestamp(substr(to_char(replace(P_TRANSACTION_TAB(i).TRANSLINK_TRA_START_TIME, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                  ,to_timestamp(substr(to_char(replace(P_TRANSACTION_TAB(i).TRA_END_TIME, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                  ,SUBSTR(P_TRANSACTION_TAB(i).BARCODE_NUMBER, 1, 100)
                  ,case when upper(P_TRANSACTION_TAB(i).CANCEL_FLAG)='TRUE' THEN 'Y'
                    when  upper(P_TRANSACTION_TAB(i).CANCEL_FLAG)='FALSE' THEN 'N'
                    else SUBSTR(P_TRANSACTION_TAB(i).CANCEL_FLAG,1,1) end
                  ,SUBSTR(P_TRANSACTION_TAB(i).CUR_CODE, 1, 3)
                  ,SUBSTR(P_TRANSACTION_TAB(i).CUSTOMER_GROUP, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).DIVISION_CODE, 1, 10)
                  ,case when upper(P_TRANSACTION_TAB(i).HOME_DELIVERY_FLAG)='TRUE' THEN 'Y'
                    when  upper(P_TRANSACTION_TAB(i).HOME_DELIVERY_FLAG)='FALSE' THEN 'N'
                    else SUBSTR(P_TRANSACTION_TAB(i).HOME_DELIVERY_FLAG,1,1) end
                  ,SUBSTR(P_TRANSACTION_TAB(i).LOYALTY_ACCOUNT_ID, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).LOYALTY_PROGRAM_ID, 1, 50)
                  ,case when upper(P_TRANSACTION_TAB(i).OFFLINE_FLAG)='TRUE' THEN 'Y'
                    when  upper(P_TRANSACTION_TAB(i).OFFLINE_FLAG)='FALSE' THEN 'N'
                    else SUBSTR(P_TRANSACTION_TAB(i).OFFLINE_FLAG,1,1) end
                  ,SUBSTR(P_TRANSACTION_TAB(i).OPERATOR_NAME, 1, 100)
                  ,SUBSTR(P_TRANSACTION_TAB(i).OPERATOR_TYPE, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).ORGANISATION_HIERARCHY, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).ORGANISATION_HIERARCHY_ID, 1, 10)
                  ,SUBSTR(P_TRANSACTION_TAB(i).ORGANISATION_HIERARCHY_LEVEL, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).PRICE_COMMUNICATION_AREA, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).TAX_REQUEST_RESULT, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).TAX_REQUEST_TYPE, 1, 30)
                  ,SUBSTR(P_TRANSACTION_TAB(i).TILL_ID, 1, 30)
                  ,SYSDATE
                  ,SYSDATE
                  );
              EXCEPTION
                 WHEN OTHERS THEN
                   if sqlcode = -12899 then
                       V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                   elsif sqlcode = -00001 then
                       P_RESPONSE := 'Duplicate receipt.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE;
                       PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                       Rollback;
                       RETURN;
                   elsif sqlcode = -01400 then
                       V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                   else
                       V_ERR_MSG := SQLERRM;
                   end if;
                   PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                   PV_STEP_SEQ := PV_STEP_SEQ + 1;
              END;
              -------------
              V_TRA_ITEM_TAB              := P_TRANSACTION_TAB(i).TRA_ITEM_TAB;
              V_TRA_CUSTOMER_TAB          := P_TRANSACTION_TAB(i).TRA_CUSTOMER_TAB;
              V_TRA_DISC_TAB              := P_TRANSACTION_TAB(i).TRA_DISC_TAB;
              V_TRA_GIFT_CERT_TAB         := P_TRANSACTION_TAB(i).TRA_GIFT_CERT_TAB;
              V_TRA_LOYALTY_RW_TAB        := P_TRANSACTION_TAB(i).TRA_LOYALTY_RW_TAB;
              V_TRA_SIGNATURE_TAB         := P_TRANSACTION_TAB(i).TRA_SIGNATURE_TAB;
              V_TRA_SURVEY_TAB            := P_TRANSACTION_TAB(i).TRA_SURVEY_TAB;
              V_TRA_TAX_TAB               := P_TRANSACTION_TAB(i).TRA_TAX_TAB;
              V_TRA_TOTAL_TAB             := P_TRANSACTION_TAB(i).TRA_TOTAL_TAB;
              V_TRA_TENDER_TAB            := P_TRANSACTION_TAB(i).TRA_TENDER_TAB;
              -----------
              -- Loading CEM_PL_TRA_ITEM_T
              V_TAB_NAME := 'CEM_PL_TRA_ITEM_T';

              IF V_TRA_ITEM_TAB IS NOT NULL
              THEN
                  FOR i2 IN 1..V_TRA_ITEM_TAB.COUNT
                  LOOP
                      BEGIN
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_LINE_SEQ_NO'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_LINE_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).ACTUAL_ITEM_PRICE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.ACTUAL_ITEM_PRICE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'ACTUAL_ITEM_PRICE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).DISC_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.DISC_AMOUNT'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'DISC_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).FAMILY_ITEM_PRICE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.FAMILY_ITEM_PRICE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'FAMILY_ITEM_PRICE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).INVENTORY_RESERVATION_ID, p_datatype => 'N', p_size => '10,0', P_REMARK => V_TAB_NAME||'.INVENTORY_RESERVATION_ID'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'INVENTORY_RESERVATION_ID', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).ITEM_QTY, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.ITEM_QTY'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'ITEM_QTY', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).NEW_ITEM_PRICE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.NEW_ITEM_PRICE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'NEW_ITEM_PRICE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).REGULAR_ITEM_PRICE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.REGULAR_ITEM_PRICE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'REGULAR_ITEM_PRICE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).SALES_VALUE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.SALES_VALUE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'SALES_VALUE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).SEL_ITEM_PRICE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.SEL_ITEM_PRICE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'SEL_ITEM_PRICE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).SPECIAL_ORDER_NO, p_datatype => 'N', p_size => '10,0', P_REMARK => V_TAB_NAME||'.SPECIAL_ORDER_NO'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'SPECIAL_ORDER_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).TOT_DISC_VALUE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TOT_DISC_VALUE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'TOT_DISC_VALUE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).TRANSLINK_TRA_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRANSLINK_TRA_SEQ_NO'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'TRANSLINK_TRA_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).TRANSLINK_WORKSTATION_ID, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRANSLINK_WORKSTATION_ID'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'TRANSLINK_WORKSTATION_ID', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).UNIT_ITEM_PRICE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.UNIT_ITEM_PRICE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'UNIT_ITEM_PRICE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          if not(validate_col_value_f1(p_col_value => V_TRA_ITEM_TAB(i2).WANTED_ITEM_PRICE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.WANTED_ITEM_PRICE'||': ' ))
                          then
                            PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_ITEM_T', P_ERROR_MESSAGE => V_datatype_err||'WANTED_ITEM_PRICE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                          end if;
                          ------------
                          INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_ITEM_T
                          (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,TRA_LINE_SEQ_NO
                          ,TRANSLINK_BUSINESS_DAY
                          ,ACTUAL_ITEM_PRICE
                          ,DISC_AMOUNT
                          ,FAMILY_ITEM_PRICE
                          ,INVENTORY_RESERVATION_ID
                          ,ITEM_QTY
                          ,NEW_ITEM_PRICE
                          ,REGULAR_ITEM_PRICE
                          ,SALES_VALUE
                          ,SEL_ITEM_PRICE
                          ,SPECIAL_ORDER_NO
                          ,TOT_DISC_VALUE
                          ,TRANSLINK_TRA_SEQ_NO
                          ,TRANSLINK_WORKSTATION_ID
                          ,UNIT_ITEM_PRICE
                          ,WANTED_ITEM_PRICE
                          ,TRANSLINK_TRA_START_TIME
                          ,TRA_LINE_START_TIME
                          ,ARTS_ITEM_TYPE
                          ,ASIS_FLAG
                          ,BULKY_ITEM_FLAG
                          ,CANCELLED_PREPAYMENT_FLAG
                          ,DESCRIPTION
                          ,DISPOSAL_METHOD
                          ,ENTRY_METHOD
                          ,FAMILY_ITEM_FLAG
                          ,ITEM_NO
                          ,ITEM_NOT_ON_FILE_FLAG
                          ,NEW_PRICE_FLAG
                          ,PA_NO
                          ,POS_ID_TYPE
                          ,POS_ITEM_ID
                          ,RETURN_ID
                          ,SALES_COND_CODE
                          ,SALES_COND_ENTRY_METHOD
                          ,SEL_ITEM_PRICE_TYPE
                          ,SUPPLIER_ID
                          ,TAX_ITEM_CAT_CODE
                          ,TRANSLINK_BU_CODE
                          ,TRANSLINK_BU_TYPE
                          ,TRANSLINK_REASON_CODE
                          ,TRA_TYPE
                          ,UOM_CODE_QTY
                          ,VOID_FLAG
                          ,WANTED_ITEM_PRICE_TYPE
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE
                          )
                          VALUES
                          (V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,to_number(V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO)
                          ,to_timestamp(substr(to_char(replace(V_TRA_ITEM_TAB(i2).TRANSLINK_BUSINESS_DAY, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                          ,to_number(V_TRA_ITEM_TAB(i2).ACTUAL_ITEM_PRICE)
                          ,to_number(V_TRA_ITEM_TAB(i2).DISC_AMOUNT)
                          ,to_number(V_TRA_ITEM_TAB(i2).FAMILY_ITEM_PRICE)
                          ,to_number(V_TRA_ITEM_TAB(i2).INVENTORY_RESERVATION_ID)
                          ,to_number(V_TRA_ITEM_TAB(i2).ITEM_QTY)
                          ,to_number(V_TRA_ITEM_TAB(i2).NEW_ITEM_PRICE)
                          ,to_number(V_TRA_ITEM_TAB(i2).REGULAR_ITEM_PRICE)
                          ,to_number(V_TRA_ITEM_TAB(i2).SALES_VALUE)
                          ,to_number(V_TRA_ITEM_TAB(i2).SEL_ITEM_PRICE)
                          ,to_number(V_TRA_ITEM_TAB(i2).SPECIAL_ORDER_NO)
                          ,to_number(V_TRA_ITEM_TAB(i2).TOT_DISC_VALUE)
                          ,to_number(V_TRA_ITEM_TAB(i2).TRANSLINK_TRA_SEQ_NO)
                          ,to_number(V_TRA_ITEM_TAB(i2).TRANSLINK_WORKSTATION_ID)
                          ,to_number(V_TRA_ITEM_TAB(i2).UNIT_ITEM_PRICE)
                          ,to_number(V_TRA_ITEM_TAB(i2).WANTED_ITEM_PRICE)
                          ,to_timestamp(substr(to_char(replace(V_TRA_ITEM_TAB(i2).TRANSLINK_TRA_START_TIME, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                          ,to_timestamp(substr(to_char(replace(V_TRA_ITEM_TAB(i2).TRA_LINE_START_TIME, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).ARTS_ITEM_TYPE, 1, 10)
                          ,case when UPPER(V_TRA_ITEM_TAB(i2).ASIS_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_ITEM_TAB(i2).ASIS_FLAG)='FALSE' then 'N'  else SUBSTR(V_TRA_ITEM_TAB(i2).ASIS_FLAG,1,1) end
                          ,case when UPPER(V_TRA_ITEM_TAB(i2).BULKY_ITEM_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_ITEM_TAB(i2).BULKY_ITEM_FLAG)='FALSE' then 'N'  else SUBSTR(V_TRA_ITEM_TAB(i2).BULKY_ITEM_FLAG,1,1) end
                          ,case when UPPER(V_TRA_ITEM_TAB(i2).CANCELLED_PREPAYMENT_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_ITEM_TAB(i2).CANCELLED_PREPAYMENT_FLAG)='FALSE' then 'N'  else SUBSTR(V_TRA_ITEM_TAB(i2).CANCELLED_PREPAYMENT_FLAG,1,1) end
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).DESCRIPTION, 1, 300)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).DISPOSAL_METHOD, 1, 30)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).ENTRY_METHOD, 1, 100)
                          ,case when UPPER(V_TRA_ITEM_TAB(i2).FAMILY_ITEM_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_ITEM_TAB(i2).FAMILY_ITEM_FLAG)='FALSE' then 'N'  else SUBSTR(V_TRA_ITEM_TAB(i2).FAMILY_ITEM_FLAG,1,1) end
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).ITEM_NO, 1, 15)
                          ,case when UPPER(V_TRA_ITEM_TAB(i2).ITEM_NOT_ON_FILE_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_ITEM_TAB(i2).ITEM_NOT_ON_FILE_FLAG)='FALSE' then 'N'  else SUBSTR(V_TRA_ITEM_TAB(i2).ITEM_NOT_ON_FILE_FLAG,1,1) end
                          ,case when UPPER(V_TRA_ITEM_TAB(i2).NEW_PRICE_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_ITEM_TAB(i2).NEW_PRICE_FLAG)='FALSE' then 'N'  else SUBSTR(V_TRA_ITEM_TAB(i2).NEW_PRICE_FLAG,1,1) end
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).PA_NO, 1, 4)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).POS_ID_TYPE, 1, 10)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).POS_ITEM_ID, 1, 30)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).RETURN_ID, 1, 30)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).SALES_COND_CODE, 1, 30)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).SALES_COND_ENTRY_METHOD, 1, 30)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).SEL_ITEM_PRICE_TYPE, 1, 30)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).SUPPLIER_ID, 1, 5)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).TAX_ITEM_CAT_CODE, 1, 20)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).TRANSLINK_BU_CODE, 1, 5)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).TRANSLINK_BU_TYPE, 1, 3)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).TRANSLINK_REASON_CODE, 1, 30)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).TRA_TYPE, 1, 30)
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).UOM_CODE_QTY, 1, 10)
                          ,case when UPPER(V_TRA_ITEM_TAB(i2).VOID_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_ITEM_TAB(i2).VOID_FLAG)='FALSE' then 'N'  else SUBSTR(V_TRA_ITEM_TAB(i2).VOID_FLAG,1,1) end
                          ,SUBSTR(V_TRA_ITEM_TAB(i2).WANTED_ITEM_PRICE_TYPE, 1, 30)
                          ,SYSDATE
                          ,SYSDATE
                          );
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                           if sqlcode = -12899 then
                               V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                           elsif sqlcode = -00001 then
                               P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO;
                               PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                               Rollback;
                               RETURN;
                           elsif sqlcode = -01400 then
                               V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                           else
                               V_ERR_MSG := SQLERRM;
                           end if;
                           PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                           PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                      -----------
                      V_TRA_LINE_TAX_TAB := DH_BUS_TRANS.TRA_LINE_TAX_TAB();
                      V_TRA_LINE_DISC_TAB    := V_TRA_ITEM_TAB(i2).TRA_LINE_DISC_TAB;
                      V_TRA_LINE_TAX_TAB     := V_TRA_ITEM_TAB(i2).TRA_LINE_TAX_TAB;
                      V_TRA_LINE_SUBITEM_TAB := V_TRA_ITEM_TAB(i2).TRA_LINE_SUBITEM_TAB;
                      -----------
                      --==============================================================================================================
                      --Loading CEM_PL_TRA_LINE_DISC_T table
                      V_TAB_NAME := 'CEM_PL_TRA_LINE_DISC_T';
                      IF V_TRA_LINE_DISC_TAB IS NOT NULL
                      THEN
                          FOR i3 IN 1..V_TRA_LINE_DISC_TAB.COUNT
                          LOOP
                             BEGIN
                                if not(validate_col_value_f1(p_col_value => V_TRA_LINE_DISC_TAB(i3).TRA_LINE_DISC_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_LINE_DISC_SEQ_NO'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_DISC_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_LINE_DISC_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_DISC_TAB(i3).DISC_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.DISC_AMOUNT'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_DISC_T', P_ERROR_MESSAGE => V_datatype_err||'DISC_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_DISC_TAB(i3).PREVIOUS_PRICE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.PREVIOUS_PRICE'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_DISC_T', P_ERROR_MESSAGE => V_datatype_err||'PREVIOUS_PRICE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;

                                 INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_LINE_DISC_T
                                 (TRA_SEQ_NO
                                  ,TRA_START_TIME
                                  ,WORKSTATION_ID
                                  ,BU_CODE
                                  ,BU_TYPE
                                  ,TRA_LINE_SEQ_NO
                                  ,TRA_LINE_DISC_SEQ_NO
                                  ,DISC_AMOUNT
                                  ,PREVIOUS_PRICE
                                  ,ACTION
                                  ,CUR_CODE
                                  ,DISC_TYPE
                                  ,METHOD_CODE
                                  ,REASON_CODE
                                  ,SAPP_INSERT_DATE
                                  ,SAPP_UPDATE_DATE)
                                  VALUES(V_TRA_SEQ_NO
                                  ,V_TRA_START_TIME
                                  ,V_WORKSTATION_ID
                                  ,V_BU_CODE
                                  ,v_BU_TYPE
                                  ,to_number(V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO)
                                  ,to_number(V_TRA_LINE_DISC_TAB(i3).TRA_LINE_DISC_SEQ_NO)
                                  ,to_number(V_TRA_LINE_DISC_TAB(i3).DISC_AMOUNT)
                                  ,to_number(V_TRA_LINE_DISC_TAB(i3).PREVIOUS_PRICE)
                                  ,SUBSTR(V_TRA_LINE_DISC_TAB(i3).ACTION, 1, 30)
                                  ,SUBSTR(V_TRA_LINE_DISC_TAB(i3).CUR_CODE, 1, 3)
                                  ,SUBSTR(V_TRA_LINE_DISC_TAB(i3).DISC_TYPE, 1, 100)
                                  ,SUBSTR(V_TRA_LINE_DISC_TAB(i3).METHOD_CODE, 1, 30)
                                  ,SUBSTR(V_TRA_LINE_DISC_TAB(i3).REASON_CODE, 1, 100)
                                  ,SYSDATE
                                  ,SYSDATE);
                                --  PL_INT_LOG_P1(P_LINE_IND => 'N', P_TABLE_NAME => 'CEM_PL_TRA_LINE_DISC_T', P_ERROR_MESSAGE => 'Inserted', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                              EXCEPTION
                                 WHEN INVALID_NUMBER THEN
                                    NULL;
                                 WHEN OTHERS THEN
                                     if sqlcode = -12899 then
                                     V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                                 elsif sqlcode = -00001 then
                                     P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO||'//TRA_LINE_DISC_SEQ_NO:'||V_TRA_LINE_DISC_TAB(i3).TRA_LINE_DISC_SEQ_NO;
                                     PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                     Rollback;
                                     RETURN;
                                 elsif sqlcode = -01400 then
                                     V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                                 else
                                     V_ERR_MSG := SQLERRM;
                                 end if;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                   PV_STEP_SEQ := PV_STEP_SEQ + 1;
                              END;
                          END LOOP; --CEM_PL_TRA_LINE_DISC_T loop ends
                      END IF;
                      ----------
                      --==============================================================================================================
                      --Loading CEM_PL_TRA_LINE_TAX_T table
                      V_TAB_NAME := 'CEM_PL_TRA_LINE_TAX_T';
                      IF V_TRA_LINE_TAX_TAB IS NOT NULL
                      THEN
                          FOR i4 IN 1..V_TRA_LINE_TAX_TAB.COUNT
                          LOOP
                             BEGIN

                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_TAX_TAB(i4).TRA_LINE_TAX_SEQ_NO, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TRA_LINE_TAX_SEQ_NO'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_LINE_TAX_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_TAX_TAB(i4).TAX_DATE, p_datatype => 'D', p_size => '0', P_REMARK => V_TAB_NAME||'.TAX_DATE'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_DATE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_TAX_TAB(i4).TAXABLE_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAXABLE_AMOUNT'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAXABLE_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_TAX_TAB(i4).TAXABLE_PERCENTAGE, p_datatype => 'N', p_size => '10,6', P_REMARK => V_TAB_NAME||'.TAXABLE_PERCENTAGE'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAXABLE_PERCENTAGE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_TAX_TAB(i4).TAX_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAX_AMOUNT'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_TAX_TAB(i4).TAX_EXEMPT_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAX_EXEMPT_AMOUNT'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_EXEMPT_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_TAX_TAB(i4).TAX_PRINTED_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAX_PRINTED_AMOUNT'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_PRINTED_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_TAX_TAB(i4).TAX_RATE, p_datatype => 'N', p_size => '10,6', P_REMARK => V_TAB_NAME||'.TAX_RATE'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_RATE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                -------
                                 INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_LINE_TAX_T
                                 (TRA_SEQ_NO
                                  ,TRA_START_TIME
                                  ,WORKSTATION_ID
                                  ,BU_CODE
                                  ,BU_TYPE
                                  ,TRA_LINE_SEQ_NO
                                  ,TRA_LINE_TAX_SEQ_NO
                                  ,TAX_DATE
                                  ,TAXABLE_AMOUNT
                                  ,TAXABLE_PERCENTAGE
                                  ,TAX_AMOUNT
                                  ,TAX_EXEMPT_AMOUNT
                                  ,TAX_PRINTED_AMOUNT
                                  ,TAX_RATE
                                  ,CUR_CODE
                                  ,TAXABLE_AMOUNT_INCL_VAT_FLAG
                                  ,TAX_AUTHORITY
                                  ,TAX_CALC_METHOD
                                  ,TAX_DISPLAY_NAME
                                  ,TAX_EXEMPT_CUSTOMER_ID
                                  ,TAX_EXEMPT_REASON_CODE
                                  ,TAX_GROUP
                                  ,TAX_LOCATION
                                  ,TAX_RATE_CAT_CODE
                                  ,TAX_RULE
                                  ,TAX_TYPE
                                  ,TAX_TYPE_CODE
                                  ,SAPP_INSERT_DATE
                                  ,SAPP_UPDATE_DATE)
                                  VALUES(V_TRA_SEQ_NO
                                  ,V_TRA_START_TIME
                                  ,V_WORKSTATION_ID
                                  ,V_BU_CODE
                                  ,v_BU_TYPE
                                  ,to_number(V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO)
                                  ,to_number(V_TRA_LINE_TAX_TAB(i4).TRA_LINE_TAX_SEQ_NO)
                                  ,to_timestamp(substr(to_char(replace(V_TRA_LINE_TAX_TAB(i4).TAX_DATE, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                                  ,to_number(V_TRA_LINE_TAX_TAB(i4).TAXABLE_AMOUNT)
                                  ,to_number(V_TRA_LINE_TAX_TAB(i4).TAXABLE_PERCENTAGE)
                                  ,to_number(V_TRA_LINE_TAX_TAB(i4).TAX_AMOUNT)
                                  ,to_number(V_TRA_LINE_TAX_TAB(i4).TAX_EXEMPT_AMOUNT)
                                  ,to_number(V_TRA_LINE_TAX_TAB(i4).TAX_PRINTED_AMOUNT)
                                  ,to_number(V_TRA_LINE_TAX_TAB(i4).TAX_RATE)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).CUR_CODE, 1, 3)
                                  ,case when upper(V_TRA_LINE_TAX_TAB(i4).TAXABLE_AMOUNT_INCL_VAT_FLAG)='TRUE' then 'Y' when upper(V_TRA_LINE_TAX_TAB(i4).TAXABLE_AMOUNT_INCL_VAT_FLAG)='FALSE' then 'N' else SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAXABLE_AMOUNT_INCL_VAT_FLAG,1,1) end
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_AUTHORITY, 1, 100)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_CALC_METHOD, 1, 30)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_DISPLAY_NAME, 1, 30)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_EXEMPT_CUSTOMER_ID, 1, 100)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_EXEMPT_REASON_CODE, 1, 100)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_GROUP, 1, 100)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_LOCATION, 1, 100)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_RATE_CAT_CODE, 1, 30)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_RULE, 1, 100)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_TYPE, 1, 30)
                                  ,SUBSTR(V_TRA_LINE_TAX_TAB(i4).TAX_TYPE_CODE, 1, 30)
                                  ,SYSDATE
                                  ,SYSDATE);
                              EXCEPTION
                                 WHEN INVALID_NUMBER THEN
                                    NULL;
                                 WHEN OTHERS THEN
                                     if sqlcode = -12899 then
                                         V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                                     elsif sqlcode = -00001 then
                                         P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO||'//TRA_LINE_SEQ_NO:'||V_TRA_LINE_TAX_TAB(i4).TRA_LINE_TAX_SEQ_NO;
                                         PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                         Rollback;
                                         RETURN;
                                     elsif sqlcode = -01400 then
                                         V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                                     else
                                         V_ERR_MSG := SQLERRM;
                                     end if;
                                     PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                       PV_STEP_SEQ := PV_STEP_SEQ + 1;
                              END;
                          END LOOP; --CEM_PL_TRA_LINE_TAX_T loop ends
                      END IF;
                      ----------
                      --==============================================================================================================
                      --Loading CEM_PL_TRA_LINE_SUBITEM_T table
                      V_TAB_NAME := 'CEM_PL_TRA_LINE_SUBITEM_T';
                      IF V_TRA_LINE_SUBITEM_TAB IS NOT NULL
                      THEN
                          FOR i5 IN 1..V_TRA_LINE_SUBITEM_TAB.COUNT
                          LOOP
                             BEGIN
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_SUBITEM_TAB(i5).SUBITEM_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.SUBITEM_SEQ_NO'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_SUBITEM_T', P_ERROR_MESSAGE => V_datatype_err||'SUBITEM_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_SUBITEM_TAB(i5).ITEM_QTY, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.ITEM_QTY'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_SUBITEM_T', P_ERROR_MESSAGE => V_datatype_err||'ITEM_QTY', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                 if not(validate_col_value_f1(p_col_value => V_TRA_LINE_SUBITEM_TAB(i5).SALES_VALUE, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.SALES_VALUE'||': ' ))
                                then
                                  PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_SUBITEM_T', P_ERROR_MESSAGE => V_datatype_err||'SALES_VALUE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                  PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                end if;
                                -----------
                                 INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_LINE_SUBITEM_T
                                 (TRA_SEQ_NO
                                  ,TRA_START_TIME
                                  ,WORKSTATION_ID
                                  ,BU_CODE
                                  ,BU_TYPE
                                  ,TRA_LINE_SEQ_NO
                                  ,SUBITEM_SEQ_NO
                                  ,ITEM_QTY
                                  ,SALES_VALUE
                                  ,DESCRIPTION
                                  ,ITEM_NO
                                  ,PA_NO
                                  ,UOM_CODE_QTY
                                  ,SAPP_INSERT_DATE
                                  ,SAPP_UPDATE_DATE)
                                  VALUES(V_TRA_SEQ_NO
                                  ,V_TRA_START_TIME
                                  ,V_WORKSTATION_ID
                                  ,V_BU_CODE
                                  ,v_BU_TYPE
                                  ,to_number(V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO)
                                  ,to_number(V_TRA_LINE_SUBITEM_TAB(i5).SUBITEM_SEQ_NO)
                                  ,to_number(V_TRA_LINE_SUBITEM_TAB(i5).ITEM_QTY)
                                  ,to_number(V_TRA_LINE_SUBITEM_TAB(i5).SALES_VALUE)
                                  ,SUBSTR(V_TRA_LINE_SUBITEM_TAB(i5).DESCRIPTION, 1, 300)
                                  ,SUBSTR(V_TRA_LINE_SUBITEM_TAB(i5).ITEM_NO, 1, 15)
                                  ,SUBSTR(V_TRA_LINE_SUBITEM_TAB(i5).PA_NO, 1, 4)
                                  ,SUBSTR(V_TRA_LINE_SUBITEM_TAB(i5).UOM_CODE_QTY, 1, 10)
                                  ,SYSDATE
                                  ,SYSDATE);
                              EXCEPTION
                                 WHEN INVALID_NUMBER THEN
                                    NULL;
                                 WHEN OTHERS THEN
                                     if sqlcode = -12899 then
                                         V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                                     elsif sqlcode = -00001 then
                                         P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO||'//SUBITEM_SEQ_NO:'||V_TRA_LINE_SUBITEM_TAB(i5).SUBITEM_SEQ_NO;
                                         PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                         Rollback;
                                         RETURN;
                                     elsif sqlcode = -01400 then
                                         V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                                     else
                                         V_ERR_MSG := SQLERRM;
                                     end if;
                                     PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                       PV_STEP_SEQ := PV_STEP_SEQ + 1;
                              END; --CEM_PL_TRA_LINE_SUBITEM_T loop end.

                              V_TRA_LINE_SUBITEM_TAX_TAB := V_TRA_LINE_SUBITEM_TAB(i5).TRA_SUBITEM_TAX_TAB;
                              ----------
                              --==============================================================================================================
                              --Loading CEM_PL_TRA_LINE_SI_TAX_T table
                              V_TAB_NAME := 'CEM_PL_TRA_LINE_SI_TAX_T';
                              IF V_TRA_LINE_SUBITEM_TAX_TAB IS NOT NULL
                              THEN
                                  FOR i6 IN 1..V_TRA_LINE_SUBITEM_TAX_TAB.COUNT
                                  LOOP
                                     BEGIN
                                        if not(validate_col_value_f1(p_col_value => V_TRA_LINE_SUBITEM_TAX_TAB(i6).SUBITEM_TAX_IDX, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.SUBITEM_TAX_IDX'||': ' ))
                                        then
                                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_SI_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'SUBITEM_TAX_IDX', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                        end if;
                                         if not(validate_col_value_f1(p_col_value => V_TRA_LINE_SUBITEM_TAX_TAB(i6).TAX_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAX_AMOUNT'||': ' ))
                                        then
                                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LINE_SI_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                        end if;
                                        -------
                                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_LINE_SI_TAX_T
                                         (TRA_SEQ_NO
                                          ,TRA_START_TIME
                                          ,WORKSTATION_ID
                                          ,BU_CODE
                                          ,BU_TYPE
                                          ,TRA_LINE_SEQ_NO
                                          ,SUBITEM_SEQ_NO
                                          ,SUBITEM_TAX_IDX
                                          ,TAX_AMOUNT
                                          ,TAX_GROUP
                                          ,SAPP_INSERT_DATE
                                          ,SAPP_UPDATE_DATE)
                                          VALUES(V_TRA_SEQ_NO
                                          ,V_TRA_START_TIME
                                          ,V_WORKSTATION_ID
                                          ,V_BU_CODE
                                          ,v_BU_TYPE
                                          ,to_number(V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO)
                                          ,to_number(V_TRA_LINE_SUBITEM_TAB(i5).SUBITEM_SEQ_NO)
                                          ,to_number(V_TRA_LINE_SUBITEM_TAX_TAB(i6).SUBITEM_TAX_IDX)
                                          ,to_number(V_TRA_LINE_SUBITEM_TAX_TAB(i6).TAX_AMOUNT)
                                          ,SUBSTR(V_TRA_LINE_SUBITEM_TAX_TAB(i6).TAX_GROUP, 1, 100)
                                          ,SYSDATE
                                          ,SYSDATE);
                                      EXCEPTION
                                         WHEN INVALID_NUMBER THEN
                                            NULL;
                                         WHEN OTHERS THEN
                                             if sqlcode = -12899 then
                                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                                             elsif sqlcode = -00001 then
                                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_ITEM_TAB(i2).TRA_LINE_SEQ_NO||'//SUBITEM_SEQ_NO:'||V_TRA_LINE_SUBITEM_TAB(i5).SUBITEM_SEQ_NO||'//SUBITEM_TAX_IDX:'||V_TRA_LINE_SUBITEM_TAX_TAB(i6).SUBITEM_TAX_IDX;
                                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                                 Rollback;
                                                 RETURN;
                                             elsif sqlcode = -01400 then
                                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                                             else
                                                 V_ERR_MSG := SQLERRM;
                                             end if;
                                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                            PV_STEP_SEQ := PV_STEP_SEQ + 1;
                                      END;
                                  ---------
                                  END LOOP; -- CEM_PL_TRA_LINE_SI_TAX_T loop ends
                              END IF;
                              ----------
                              --==============================================================================================================
                          END LOOP; -- CEM_PL_TRA_LINE_SUBITEM_T loop ends
                      END IF;
                      ---------
                  END LOOP; -- CEM_PL_TRA_ITEM_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_CUSTOMER_T table
              V_TAB_NAME := 'CEM_PL_TRA_CUSTOMER_T';
             -- PL_INT_LOG_P1(P_LINE_IND => 'N', P_TABLE_NAME => 'CEM_PL_TRA_CUSTOMER_T', P_ERROR_MESSAGE => 'Begin CEM_PL_TRA_CUSTOMER_T', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
              IF V_TRA_CUSTOMER_TAB IS NOT NULL
              THEN
                 --PL_INT_LOG_P1(P_LINE_IND => 'N', P_TABLE_NAME => 'CEM_PL_TRA_CUSTOMER_T', P_ERROR_MESSAGE => 'CEM_PL_TRA_CUSTOMER_T', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                  FOR i7 IN 1..V_TRA_CUSTOMER_TAB.COUNT
                  LOOP
                    -- PL_INT_LOG_P1(P_LINE_IND => 'N', P_TABLE_NAME => 'CEM_PL_TRA_CUSTOMER_T', P_ERROR_MESSAGE => 'CUSTOMER_IDX:'||V_TRA_CUSTOMER_TAB(i7).CUSTOMER_IDX||' WORKER_ID:'||V_TRA_CUSTOMER_TAB(i7).WORKER_ID, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                     BEGIN
                         if not(validate_col_value_f1(p_col_value => V_TRA_CUSTOMER_TAB(i7).CUSTOMER_IDX, p_datatype => 'N', p_size => '22,0', P_REMARK => V_TAB_NAME||'.CUSTOMER_IDX'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_CUSTOMER_T', P_ERROR_MESSAGE => V_datatype_err||'CUSTOMER_IDX', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_CUSTOMER_TAB(i7).WORKER_ID, p_datatype => 'N', p_size => '10,0', P_REMARK => V_TAB_NAME||'.WORKER_ID'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_CUSTOMER_T', P_ERROR_MESSAGE => V_datatype_err||'WORKER_ID', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        -------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_CUSTOMER_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,CUSTOMER_IDX
                          ,WORKER_ID
                          ,CITY
                          ,COUNTRY
                          ,CUSTOMER_FIRSTNAME
                          ,CUSTOMER_FULLNAME
                          ,CUSTOMER_ID
                          ,CUSTOMER_LASTNAME
                          ,POSTAL_CODE
                          ,PROVINCE
                          ,STATE
                          ,STREET_ADDRESS1
                          ,STREET_ADDRESS2
                          ,STREET_ADDRESS3
                          ,STREET_ADDRESS4
                          ,STREET_ADDRESS5
                          ,STREET_ADDRESS6
                          ,STREET_ADDRESS7
                          ,TAX_CERTIFICATE
                          ,WORKER_FULLNAME
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,to_number(V_TRA_CUSTOMER_TAB(i7).CUSTOMER_IDX)
                          ,to_number(V_TRA_CUSTOMER_TAB(i7).WORKER_ID)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).CITY, 1, 50)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).COUNTRY, 1, 30)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).CUSTOMER_FIRSTNAME, 1, 50)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).CUSTOMER_FULLNAME, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).CUSTOMER_ID, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).CUSTOMER_LASTNAME, 1, 50)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).POSTAL_CODE, 1, 30)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).PROVINCE, 1, 50)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).STATE, 1, 50)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).STREET_ADDRESS1, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).STREET_ADDRESS2, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).STREET_ADDRESS3, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).STREET_ADDRESS4, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).STREET_ADDRESS5, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).STREET_ADDRESS6, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).STREET_ADDRESS7, 1, 100)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).TAX_CERTIFICATE, 1, 30)
                          ,SUBSTR(V_TRA_CUSTOMER_TAB(i7).WORKER_FULLNAME, 1, 100)
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                             if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//CUSTOMER_IDX:'||V_TRA_CUSTOMER_TAB(i7).CUSTOMER_IDX;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_CUSTOMER_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_TENDER_T table
              V_TAB_NAME := 'CEM_PL_TRA_TENDER_T';
              IF V_TRA_TENDER_TAB IS NOT NULL
              THEN
                  FOR i8 IN 1..V_TRA_TENDER_TAB.COUNT
                  LOOP
                     BEGIN
                         if not(validate_col_value_f1(p_col_value => V_TRA_TENDER_TAB(i8).TRA_LINE_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_LINE_SEQ_NO'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TENDER_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_LINE_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TENDER_TAB(i8).FOREIGN_CURRENCY_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.FOREIGN_CURRENCY_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TENDER_T', P_ERROR_MESSAGE => V_datatype_err||'FOREIGN_CURRENCY_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TENDER_TAB(i8).FOREIGN_CURRENCY_EXRT, p_datatype => 'N', p_size => '24,16', P_REMARK => V_TAB_NAME||'.FOREIGN_CURRENCY_EXRT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TENDER_T', P_ERROR_MESSAGE => V_datatype_err||'FOREIGN_CURRENCY_EXRT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TENDER_TAB(i8).TENDER_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TENDER_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TENDER_T', P_ERROR_MESSAGE => V_datatype_err||'TENDER_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TENDER_TAB(i8).TENDER_QTY, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TENDER_QTY'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TENDER_T', P_ERROR_MESSAGE => V_datatype_err||'TENDER_QTY', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        ------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_TENDER_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,TRA_LINE_SEQ_NO
                          ,FOREIGN_CURRENCY_AMOUNT
                          ,FOREIGN_CURRENCY_EXRT
                          ,TENDER_AMOUNT
                          ,TENDER_QTY
                          ,AUTHORIZATION_CODE
                          ,CONTROL_ORDER_ID
                          ,CREDIT_DEBIT_ACCOUNT_NO
                          ,CREDIT_DEBIT_CARD_TYPE
                          ,CREDIT_DEBIT_ISSUER_ID
                          ,CUR_CODE
                          ,ENTRY_METHOD
                          ,EXTERNAL_TENDER_TYPE
                          ,FOREIGN_CURRENCY_CUR_CODE
                          ,PURCHASE_REFERENCE_ID
                          ,TENDER_TYPE
                          ,TENDER_TYPE_CODE
                          ,VOID_FLAG
                          ,VOUCHER_DESCRIPTION
                          ,VOUCHER_SERIAL_NO
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,to_number(V_TRA_TENDER_TAB(i8).TRA_LINE_SEQ_NO)
                          ,to_number(V_TRA_TENDER_TAB(i8).FOREIGN_CURRENCY_AMOUNT)
                          ,to_number(V_TRA_TENDER_TAB(i8).FOREIGN_CURRENCY_EXRT)
                          ,to_number(V_TRA_TENDER_TAB(i8).TENDER_AMOUNT)
                          ,to_number(V_TRA_TENDER_TAB(i8).TENDER_QTY)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).AUTHORIZATION_CODE, 1, 100)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).CONTROL_ORDER_ID, 1, 100)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).CREDIT_DEBIT_ACCOUNT_NO, 1, 30)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).CREDIT_DEBIT_CARD_TYPE, 1, 10)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).CREDIT_DEBIT_ISSUER_ID, 1, 100)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).CUR_CODE, 1, 3)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).ENTRY_METHOD, 1, 100)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).EXTERNAL_TENDER_TYPE, 1, 30)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).FOREIGN_CURRENCY_CUR_CODE, 1, 3)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).PURCHASE_REFERENCE_ID, 1, 100)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).TENDER_TYPE, 1, 30)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).TENDER_TYPE_CODE, 1, 12)
                          ,case when upper(V_TRA_TENDER_TAB(i8).VOID_FLAG)='TRUE' then 'Y' when upper(V_TRA_TENDER_TAB(i8).VOID_FLAG)='FALSE' then 'N' else SUBSTR(V_TRA_TENDER_TAB(i8).VOID_FLAG,1,1) end
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).VOUCHER_DESCRIPTION, 1, 1000)
                          ,SUBSTR(V_TRA_TENDER_TAB(i8).VOUCHER_SERIAL_NO, 1, 30)
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                             if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//TRA_LINE_SEQ_NO:'||V_TRA_TENDER_TAB(i8).TRA_LINE_SEQ_NO;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_TENDER_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_TOTAL_T table
              V_TAB_NAME := 'CEM_PL_TRA_TOTAL_T';
              IF V_TRA_TOTAL_TAB IS NOT NULL
              THEN
                  FOR i9 IN 1..V_TRA_TOTAL_TAB.COUNT
                  LOOP
                     BEGIN
                         if not(validate_col_value_f1(p_col_value => V_TRA_TOTAL_TAB(i9).AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TOTAL_T', P_ERROR_MESSAGE => V_datatype_err||'AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        ------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_TOTAL_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,TYPE
                          ,AMOUNT
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,SUBSTR(V_TRA_TOTAL_TAB(i9).TYPE, 1, 30)
                          ,to_number(V_TRA_TOTAL_TAB(i9).AMOUNT)
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                             if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TYPE:'||V_TRA_TOTAL_TAB(i9).TYPE;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_TOTAL_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_SURVEY_T table
              V_TAB_NAME := 'CEM_PL_TRA_SURVEY_T';
              IF V_TRA_SURVEY_TAB IS NOT NULL
              THEN
                  FOR i10 IN 1..V_TRA_SURVEY_TAB.COUNT
                  LOOP
                     BEGIN
                        if not(validate_col_value_f1(p_col_value => V_TRA_SURVEY_TAB(i10).SURVEY_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.SURVEY_SEQ_NO'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_SURVEY_T', P_ERROR_MESSAGE => V_datatype_err||'SURVEY_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        -------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_SURVEY_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,SURVEY_ID
                          ,SURVEY_SEQ_NO
                          ,QUERY_HEADER
                          ,QUERY_VALUE
                          ,SURVEY_NAME
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,SUBSTR(V_TRA_SURVEY_TAB(i10).SURVEY_ID, 1, 30)
                          ,to_number(V_TRA_SURVEY_TAB(i10).SURVEY_SEQ_NO)
                          ,SUBSTR(V_TRA_SURVEY_TAB(i10).QUERY_HEADER, 1, 100)
                          ,SUBSTR(V_TRA_SURVEY_TAB(i10).QUERY_VALUE, 1, 100)
                          ,SUBSTR(V_TRA_SURVEY_TAB(i10).SURVEY_NAME, 1, 100)
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                             if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//SURVEY_ID:'||V_TRA_SURVEY_TAB(i10).SURVEY_ID||'//SURVEY_SEQ_NO:'||V_TRA_SURVEY_TAB(i10).SURVEY_SEQ_NO;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_SURVEY_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_DISC_T table
              V_TAB_NAME := 'CEM_PL_TRA_DISC_T';
              IF V_TRA_DISC_TAB IS NOT NULL
              THEN
                  FOR i11 IN 1..V_TRA_DISC_TAB.COUNT
                  LOOP
                     BEGIN
                         if not(validate_col_value_f1(p_col_value => V_TRA_DISC_TAB(i11).DISCOUNT_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.DISCOUNT_SEQ_NO'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_DISC_T', P_ERROR_MESSAGE => V_datatype_err||'DISCOUNT_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_DISC_TAB(i11).DISCOUNTABLE_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.DISCOUNTABLE_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_DISC_T', P_ERROR_MESSAGE => V_datatype_err||'DISCOUNTABLE_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_DISC_TAB(i11).DISC_AMOUNT, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.DISC_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_DISC_T', P_ERROR_MESSAGE => V_datatype_err||'DISC_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_DISC_TAB(i11).PERCENTAGE, p_datatype => 'N', p_size => '10,6', P_REMARK => V_TAB_NAME||'.PERCENTAGE'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_DISC_T', P_ERROR_MESSAGE => V_datatype_err||'PERCENTAGE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        ------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_DISC_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,DISCOUNT_SEQ_NO
                          ,DISCOUNTABLE_AMOUNT
                          ,DISC_AMOUNT
                          ,PERCENTAGE
                          ,ACTION
                          ,CUR_CODE
                          ,DISC_REF
                          ,DISC_TYPE
                          ,DISC_TYPE_REF
                          ,ENTRY_METHOD
                          ,EXT_DISC_TYPE
                          ,VOID_FLAG
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,to_number(V_TRA_DISC_TAB(i11).DISCOUNT_SEQ_NO)
                          ,to_number(V_TRA_DISC_TAB(i11).DISCOUNTABLE_AMOUNT)
                          ,to_number(V_TRA_DISC_TAB(i11).DISC_AMOUNT)
                          ,to_number(V_TRA_DISC_TAB(i11).PERCENTAGE)
                          ,SUBSTR(V_TRA_DISC_TAB(i11).ACTION, 1, 30)
                          ,SUBSTR(V_TRA_DISC_TAB(i11).CUR_CODE, 1, 3)
                          ,SUBSTR(V_TRA_DISC_TAB(i11).DISC_REF, 1, 30)
                          ,SUBSTR(V_TRA_DISC_TAB(i11).DISC_TYPE, 1, 100)
                          ,SUBSTR(V_TRA_DISC_TAB(i11).DISC_TYPE_REF, 1, 30)
                          ,SUBSTR(V_TRA_DISC_TAB(i11).ENTRY_METHOD, 1, 100)
                          ,SUBSTR(V_TRA_DISC_TAB(i11).EXT_DISC_TYPE, 1, 100)
                          ,case when UPPER(V_TRA_DISC_TAB(i11).VOID_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_DISC_TAB(i11).VOID_FLAG)='FALSE' then 'N'  else SUBSTR(V_TRA_DISC_TAB(i11).VOID_FLAG,1,1) end
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                             if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//DISCOUNT_SEQ_NO:'||V_TRA_DISC_TAB(i11).DISCOUNT_SEQ_NO;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_DISC_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_LOYALTY_RW_T table
              V_TAB_NAME := 'CEM_PL_TRA_LOYALTY_RW_T';
              IF V_TRA_LOYALTY_RW_TAB IS NOT NULL
              THEN
                  FOR i12 IN 1..V_TRA_LOYALTY_RW_TAB.COUNT
                  LOOP
                     BEGIN
                         if not(validate_col_value_f1(p_col_value => V_TRA_LOYALTY_RW_TAB(i12).TRA_LINE_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_LINE_SEQ_NO'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LOYALTY_RW_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_LINE_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_LOYALTY_RW_TAB(i12).DISCOUNTABLE_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.DISCOUNTABLE_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LOYALTY_RW_T', P_ERROR_MESSAGE => V_datatype_err||'DISCOUNTABLE_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_LOYALTY_RW_TAB(i12).DISC_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.DISC_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LOYALTY_RW_T', P_ERROR_MESSAGE => V_datatype_err||'DISC_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_LOYALTY_RW_TAB(i12).PERCENTAGE, p_datatype => 'N', p_size => '10,6', P_REMARK => V_TAB_NAME||'.PERCENTAGE'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_LOYALTY_RW_T', P_ERROR_MESSAGE => V_datatype_err||'PERCENTAGE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        ------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_LOYALTY_RW_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,TRA_LINE_SEQ_NO
                          ,DISCOUNTABLE_AMOUNT
                          ,DISC_AMOUNT
                          ,PERCENTAGE
                          ,ACTION
                          ,CUR_CODE
                          ,DISC_REF
                          ,DISC_TYPE
                          ,DISC_TYPE_REF
                          ,ENTRY_METHOD
                          ,EXT_DISC_TYPE
                          ,LOYALTY_ID
                          ,PROMOTION_ID
                          ,VOID_FLAG
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,to_number(V_TRA_LOYALTY_RW_TAB(i12).TRA_LINE_SEQ_NO)
                          ,to_number(V_TRA_LOYALTY_RW_TAB(i12).DISCOUNTABLE_AMOUNT)
                          ,to_number(V_TRA_LOYALTY_RW_TAB(i12).DISC_AMOUNT)
                          ,to_number(V_TRA_LOYALTY_RW_TAB(i12).PERCENTAGE)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).ACTION, 1, 30)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).CUR_CODE, 1, 3)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).DISC_REF, 1, 30)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).DISC_TYPE, 1, 100)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).DISC_TYPE_REF, 1, 30)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).ENTRY_METHOD, 1, 100)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).EXT_DISC_TYPE, 1, 100)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).LOYALTY_ID, 1, 100)
                          ,SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).PROMOTION_ID, 1, 100)
                          ,case when upper(V_TRA_LOYALTY_RW_TAB(i12).VOID_FLAG)='TRUE' then 'Y' when upper(V_TRA_LOYALTY_RW_TAB(i12).VOID_FLAG)='FALSE' then 'N' else SUBSTR(V_TRA_LOYALTY_RW_TAB(i12).VOID_FLAG,1,1) end
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                             if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_LOYALTY_RW_TAB(i12).TRA_LINE_SEQ_NO;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_LOYALTY_RW_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_GIFT_CERT_T table
              V_TAB_NAME := 'CEM_PL_TRA_GIFT_CERT_T';
              IF V_TRA_GIFT_CERT_TAB IS NOT NULL
              THEN
                  FOR i13 IN 1..V_TRA_GIFT_CERT_TAB.COUNT
                  LOOP
                     BEGIN
                         if not(validate_col_value_f1(p_col_value => V_TRA_GIFT_CERT_TAB(i13).TRA_LINE_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_LINE_SEQ_NO'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_GIFT_CERT_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_LINE_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        -------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_GIFT_CERT_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,TRA_LINE_SEQ_NO
                          ,ENTRY_METHOD
                          ,GIFT_CERT_ID
                          ,VOID_FLAG
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,to_number(V_TRA_GIFT_CERT_TAB(i13).TRA_LINE_SEQ_NO)
                          ,SUBSTR(V_TRA_GIFT_CERT_TAB(i13).ENTRY_METHOD, 1, 100)
                          ,SUBSTR(V_TRA_GIFT_CERT_TAB(i13).GIFT_CERT_ID, 1, 100)
                          ,case when UPPER(V_TRA_GIFT_CERT_TAB(i13).VOID_FLAG)='TRUE' then 'Y' when UPPER(V_TRA_GIFT_CERT_TAB(i13).VOID_FLAG)='FALSE' then 'N' else SUBSTR(V_TRA_GIFT_CERT_TAB(i13).VOID_FLAG,1,1) end
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                             if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_GIFT_CERT_TAB(i13).TRA_LINE_SEQ_NO;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_GIFT_CERT_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_TAX_T table
              V_TAB_NAME := 'CEM_PL_TRA_TAX_T';
              IF V_TRA_TAX_TAB IS NOT NULL
              THEN
                  FOR i14 IN 1..V_TRA_TAX_TAB.COUNT
                  LOOP
                     BEGIN
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TRA_LINE_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_LINE_SEQ_NO'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_LINE_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TAX_DATE, p_datatype => 'D', p_size => '0', P_REMARK => V_TAB_NAME||'.TAX_DATE'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_DATE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TAXABLE_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAXABLE_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAXABLE_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TAXABLE_PERCENTAGE, p_datatype => 'N', p_size => '10,6', P_REMARK => V_TAB_NAME||'.TAXABLE_PERCENTAGE'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAXABLE_PERCENTAGE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TAX_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAX_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TAX_EXEMPT_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAX_EXEMPT_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_EXEMPT_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TAX_PRINTED_AMOUNT, p_datatype => 'N', p_size => '17,6', P_REMARK => V_TAB_NAME||'.TAX_PRINTED_AMOUNT'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_PRINTED_AMOUNT', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TAX_RATE, p_datatype => 'N', p_size => '10,6', P_REMARK => V_TAB_NAME||'.TAX_RATE'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TAX_RATE', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                         if not(validate_col_value_f1(p_col_value => V_TRA_TAX_TAB(i14).TRA_TAX_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_TAX_SEQ_NO'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_TAX_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_TAX_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        ------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_TAX_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,TRA_LINE_SEQ_NO
                          ,TAX_DATE
                          ,TAXABLE_AMOUNT
                          ,TAXABLE_PERCENTAGE
                          ,TAX_AMOUNT
                          ,TAX_EXEMPT_AMOUNT
                          ,TAX_PRINTED_AMOUNT
                          ,TAX_RATE
                          ,TRA_TAX_SEQ_NO
                          ,CUR_CODE
                          ,ENTRY_METHOD
                          ,TAXABLE_AMOUNT_INCL_VAT_FLAG
                          ,TAX_AUTHORITY
                          ,TAX_CALC_METHOD
                          ,TAX_DISPLAY_NAME
                          ,TAX_EXEMPT_CUSTOMER_ID
                          ,TAX_EXEMPT_REASON_CODE
                          ,TAX_GROUP
                          ,TAX_LOCATION
                          ,TAX_RATE_CAT_CODE
                          ,TAX_RULE
                          ,TAX_TYPE
                          ,TAX_TYPE_CODE
                          ,VOID_FLAG
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,to_number(V_TRA_TAX_TAB(i14).TRA_LINE_SEQ_NO)
                          ,to_timestamp(substr(to_char(replace(V_TRA_TAX_TAB(i14).TAX_DATE, 'T', ' ')),1,19), 'YYYY-MM-DD HH24:MI:SS')
                          ,to_number(V_TRA_TAX_TAB(i14).TAXABLE_AMOUNT)
                          ,to_number(V_TRA_TAX_TAB(i14).TAXABLE_PERCENTAGE)
                          ,to_number(V_TRA_TAX_TAB(i14).TAX_AMOUNT)
                          ,to_number(V_TRA_TAX_TAB(i14).TAX_EXEMPT_AMOUNT)
                          ,to_number(V_TRA_TAX_TAB(i14).TAX_PRINTED_AMOUNT)
                          ,to_number(V_TRA_TAX_TAB(i14).TAX_RATE)
                          ,to_number(V_TRA_TAX_TAB(i14).TRA_TAX_SEQ_NO)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).CUR_CODE, 1, 3)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).ENTRY_METHOD, 1, 100)
                          ,case when upper(V_TRA_TAX_TAB(i14).TAXABLE_AMOUNT_INCL_VAT_FLAG)='TRUE' THEN 'Y'
                            when  upper(V_TRA_TAX_TAB(i14).TAXABLE_AMOUNT_INCL_VAT_FLAG)='FALSE' THEN 'N'
                            else SUBSTR(V_TRA_TAX_TAB(i14).TAXABLE_AMOUNT_INCL_VAT_FLAG,1,1) end
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_AUTHORITY, 1, 100)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_CALC_METHOD, 1, 30)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_DISPLAY_NAME, 1, 30)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_EXEMPT_CUSTOMER_ID, 1, 100)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_EXEMPT_REASON_CODE, 1, 100)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_GROUP, 1, 100)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_LOCATION, 1, 100)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_RATE_CAT_CODE, 1, 20)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_RULE, 1, 100)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_TYPE, 1, 300)
                          ,SUBSTR(V_TRA_TAX_TAB(i14).TAX_TYPE_CODE, 1, 30)
                          ,case when upper(V_TRA_TAX_TAB(i14).VOID_FLAG)='TRUE' THEN 'Y'
                            when  upper(V_TRA_TAX_TAB(i14).VOID_FLAG)='FALSE' THEN 'N'
                            else SUBSTR(V_TRA_TAX_TAB(i14).VOID_FLAG,1,1) end
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                             if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_TAX_TAB(i14).TRA_LINE_SEQ_NO;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_TAX_T loop ends
              END IF;
              ----------
              --Loading CEM_PL_TRA_SIGNATURE_T table
              V_TAB_NAME := 'CEM_PL_TRA_SIGNATURE_T';
              IF V_TRA_SIGNATURE_TAB IS NOT NULL
              THEN
                  FOR i15 IN 1..V_TRA_SIGNATURE_TAB.COUNT
                  LOOP
                     BEGIN
                         if not(validate_col_value_f1(p_col_value => V_TRA_SIGNATURE_TAB(i15).TRA_LINE_SEQ_NO, p_datatype => 'N', p_size => '0', P_REMARK => V_TAB_NAME||'.TRA_LINE_SEQ_NO'||': ' ))
                        then
                          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'CEM_PL_TRA_SIGNATURE_T', P_ERROR_MESSAGE => V_datatype_err||'TRA_LINE_SEQ_NO', P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                          PV_STEP_SEQ := PV_STEP_SEQ + 1;
                        end if;
                        ------
                         INSERT INTO DH_BUS_TRANS.CEM_PL_TRA_SIGNATURE_T
                         (TRA_SEQ_NO
                          ,TRA_START_TIME
                          ,WORKSTATION_ID
                          ,BU_CODE
                          ,BU_TYPE
                          ,TRA_LINE_SEQ_NO
                          ,SIGNATURE_IMAGE
                          ,SAPP_INSERT_DATE
                          ,SAPP_UPDATE_DATE)
                          VALUES(V_TRA_SEQ_NO
                          ,V_TRA_START_TIME
                          ,V_WORKSTATION_ID
                          ,V_BU_CODE
                          ,v_BU_TYPE
                          ,to_number(V_TRA_SIGNATURE_TAB(i15).TRA_LINE_SEQ_NO)
                          ,V_TRA_SIGNATURE_TAB(i15).SIGNATURE_IMAGE
                          ,SYSDATE
                          ,SYSDATE);
                      EXCEPTION
                         WHEN INVALID_NUMBER THEN
                            NULL;
                         WHEN OTHERS THEN
                            if sqlcode = -12899 then
                                 V_ERR_MSG := 'Value too large for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             elsif sqlcode = -00001 then
                                 P_RESPONSE := 'Duplicate entry for '||V_TAB_NAME||'.***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//TRA_LINE_SEQ_NO:'||V_TRA_SIGNATURE_TAB(i15).TRA_LINE_SEQ_NO;
                                 PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                                 Rollback;
                                 RETURN;
                             elsif sqlcode = -01400 then
                                 V_ERR_MSG := 'Null value for '||substr(SQLERRM, instr(SQLERRM,V_TAB_NAME)+length(V_TAB_NAME)+2);
                             else
                                 V_ERR_MSG := SQLERRM;
                             end if;
                             PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => V_ERR_MSG, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
                             PV_STEP_SEQ := PV_STEP_SEQ + 1;
                      END;
                  END LOOP; -- CEM_PL_TRA_SIGNATURE_T loop ends
              END IF;
              ---------
             V_TRA_SEQ_NO       :=  NULL;
             V_TRA_START_TIME   :=  NULL;
             V_WORKSTATION_ID   :=  NULL;
        ---------
 			  END LOOP; -- CEM_PL_TRANSACTION_T loop ends
    END IF;
    -------
    begin
        select LISTAGG(TABLE_NAME||'#'||grp_msg, '; ') WITHIN GROUP (ORDER BY  TABLE_NAME||grp_msg)
        into P_RESPONSE
        from(
        select TABLE_NAME,  LISTAGG(REMARK_MESSAGE, ', ') WITHIN GROUP (ORDER BY  REMARK_MESSAGE) grp_msg
        from CEM_PL_INT_LOG_T
        where NVL(TRA_SEQ_NO,'1') = NVL(PV_TRA_SEQ_NO,'1')
        and NVL(TRA_START_TIME,'1') = NVL(PV_TRA_START_TIME,'1')
        and NVL(WORKSTATION_ID,'1') = NVL(PV_WORKSTATION_ID,'1')
        and NVL(BU_CODE,'1')  = NVL(PV_BU_CODE,'1')
        and NVL(BU_TYPE,'1') = NVL(PV_BU_TYPE,'1')
        AND LINE_IND = 'E'
        and sapp_insert_date > sysdate - 0.45/60/24
        group by table_name);
    exception
       when no_data_found then
          P_RESPONSE := 'Transaction failed due to: '||SQLERRM;
          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'POSLOG_INTEGRATION_P1', P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
       when others then
          P_RESPONSE := 'Transaction failed due to: '||SQLERRM;
          PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => 'POSLOG_INTEGRATION_P1', P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
    end;
    if P_RESPONSE is not null then
      ROLLBACK;
      P_RESPONSE := SUBSTR('ErrorMessage: ***TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//'||P_RESPONSE, 1, 4000);
    else
      COMMIT;
      P_RESPONSE := 'SUCCESSFULLY INSERTED';
    end if;
EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       V_ERR_MSG := SUBSTR(SQLERRM,1, 4000);
       
       if PV_TRA_SEQ_NO is not null and PV_TRA_START_TIME is not null and PV_WORKSTATION_ID is not null and PV_BU_CODE is not null and PV_BU_TYPE is not null
       then
         PL_INT_LOG_P1(P_LINE_IND => 'E', P_TABLE_NAME => V_TAB_NAME, P_ERROR_MESSAGE => P_RESPONSE, P_STEP_SEQ_NO => PV_STEP_SEQ + 1, P_DEBUG_LEVEL => PV_DEBUG_LEVEL);
         V_ERR_MSG := SUBSTR('TRA_SEQ_NO:'||PV_TRA_SEQ_NO||'//TRA_START_TIME:'||PV_TRA_START_TIME||'//WORKSTATION_ID:'||PV_WORKSTATION_ID||'//BU_CODE:'||PV_BU_CODE||'//BU_TYPE:'||PV_BU_TYPE||'//'||V_TAB_NAME||'#'||V_ERR_MSG, 1, 4000);
       end if;
       P_RESPONSE := SUBSTR('ErrorMessage: ***'||V_ERR_MSG, 1, 4000);
END POSLOG_INTEGRATION_P1;

--==============================================================================
PROCEDURE POSLOG_INVALID_RECEIPT_P1(P_INVALID_RECEIPT IN DH_BUS_TRANS.INVALID_RECEIPT_TAB, P_RESPONSE OUT VARCHAR2)
is
PRAGMA AUTONOMOUS_TRANSACTION ;
/*******************************************************************************
Description: Procedure POSLOG_INVALID_RECEIPT_P1 is responsible to the load the invalid/errored receipt into to error table CEM_PL_ERROR_T.

Request #38292

History
---------
Date               Name                    Remark
-------------      ----------------        -------------------------------------
2018-07-19         Asim Khan Pathan        Creator
2018-10-03         Asim Khan Pathan        Business requested to add primary key constraint and additional column SAPP_UPDATE_DATE on CEM_PL_ERROR_T.
                                           Duplicate entry handled, SAPP_UPDATE_DATE will be filled with current timespamp.
*******************************************************************************/
    V_ERR_MSG    VARCHAR2(4000);
    V_TRA_SEQ_NO                CEM_PL_ERROR_T.TRA_SEQ_NO%TYPE;
    V_TRA_START_TIME            CEM_PL_ERROR_T.TRA_START_TIME%TYPE;
    V_WORKSTATION_ID            CEM_PL_ERROR_T.WORKSTATION_ID%TYPE;
    V_BU_CODE                   CEM_PL_ERROR_T.BU_CODE%TYPE;
    V_BU_TYPE                   CEM_PL_ERROR_T.BU_TYPE%TYPE;
BEGIN
    IF P_INVALID_RECEIPT IS NOT NULL
    THEN
       FOR i IN 1..P_INVALID_RECEIPT.COUNT
       LOOP
           INSERT INTO DH_BUS_TRANS.CEM_PL_ERROR_T
           (	TRA_SEQ_NO
          ,TRA_START_TIME
          ,WORKSTATION_ID
          ,BU_CODE
          ,BU_TYPE
          ,SOURCE_IND
          ,RECEIPT_XML
          ,REMARK_MESSAGE
          ,SAPP_INSERT_DATE
          ,SAPP_UPDATE_DATE)
          VALUES
          (	P_INVALID_RECEIPT(i).TRA_SEQ_NO
          ,P_INVALID_RECEIPT(i).TRA_START_TIME
          ,P_INVALID_RECEIPT(i).WORKSTATION_ID
          ,P_INVALID_RECEIPT(i).BU_CODE
          ,P_INVALID_RECEIPT(i).BU_TYPE
          ,P_INVALID_RECEIPT(i).SOURCE_IND
          ,P_INVALID_RECEIPT(i).RECEIPT_XML
          ,P_INVALID_RECEIPT(i).REMARK_MESSAGE
          ,SYSDATE
          ,SYSDATE);
      END LOOP;
    END IF;
    COMMIT;
    P_RESPONSE := 'SUCCESSFULLY INSERTED';

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       if sqlcode = -00001 then
          V_ERR_MSG := 'Duplicate entry for CEM_PL_ERROR_T.***TRA_SEQ_NO:'||V_TRA_SEQ_NO||'//TRA_START_TIME:'||V_TRA_START_TIME||'//WORKSTATION_ID:'||V_WORKSTATION_ID||'//BU_CODE:'||V_BU_CODE||'//BU_TYPE:'||V_BU_TYPE;
       else
          V_ERR_MSG := SUBSTR(SQLERRM,1, 4000);
       end if;

       P_RESPONSE := 'Failed due to '||V_ERR_MSG;
END POSLOG_INVALID_RECEIPT_P1;
--==============================================================================
FUNCTION GET_PURGE_RULE_VALUE_F1(P_PURGE_RULE_NAME VARCHAR2)
RETURN VARCHAR2
/*******************************************************************************
Description: Function GET_PURGE_RULE_VALUE_F1 is responsible to return the value of the given purge rule.

Request #38292

History
---------
Date               Name                    Remark
-------------      ----------------        -------------------------------------
2018-11-29         Asim Khan Pathan        Creator
*******************************************************************************/
IS
   V_RETURN    VARCHAR2(30);
BEGIN
   SELECT RULE_VALUE 
     INTO V_RETURN
     FROM DH_BUS_TRANS.CEM_PL_PURGE_CONFIG_T
    WHERE PURGE_RULE_NAME = P_PURGE_RULE_NAME
      AND RULE_IS_VALID = 'Y';
    
   RETURN V_RETURN;
EXCEPTION
   WHEN OTHERS THEN
      IF P_PURGE_RULE_NAME = 'PURGE_TRA_OLDER_THAN_DAYS'
      THEN
         V_RETURN := '14';
      END IF;   
      RETURN V_RETURN;
END GET_PURGE_RULE_VALUE_F1; 
--==============================================================================
PROCEDURE PURGE_POSLOG_TRANS_P1
IS
/*******************************************************************************
Description: Procedure PURGE_POSLOG_TRANS_P1 is responsible to the purge the poslog transaction tables data older than the 
             value set in CEM_PL_PURGE_CONFIG_T table against rule PURGE_TRA_OLDER_THAN_DAYS.

Request #38292

History
---------
Date               Name                    Remark
-------------      ----------------        -------------------------------------
2018-11-29         Asim Khan Pathan        Creator
*******************************************************************************/
    V_PURGE_TRA_OLDER_THAN_DAYS  NUMBER;
BEGIN
    BEGIN
        SELECT NVL(TO_NUMBER(GET_PURGE_RULE_VALUE_F1(P_PURGE_RULE_NAME => 'PURGE_TRA_OLDER_THAN_DAYS')),14)
          INTO V_PURGE_TRA_OLDER_THAN_DAYS
          FROM DUAL;
    EXCEPTION
        WHEN OTHERS
        THEN
            V_PURGE_TRA_OLDER_THAN_DAYS := 14;
    END;        
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_CUSTOMER_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_DISC_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_GIFT_CERT_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_ITEM_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_LINE_DISC_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_LINE_SI_TAX_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_LINE_SUBITEM_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_LINE_TAX_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_LOYALTY_RW_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_SIGNATURE_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_SURVEY_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_TAX_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_TENDER_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRA_TOTAL_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_TRANSACTION_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_ERROR_T 
     WHERE SAPP_UPDATE_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
    
    DELETE FROM  DH_BUS_TRANS.CEM_PL_INT_LOG_T 
     WHERE SAPP_INSERT_DATE < SYSDATE - V_PURGE_TRA_OLDER_THAN_DAYS ;
     
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
       ROLLBACK;
END PURGE_POSLOG_TRANS_P1;  
END;
/