--------------------------------------------------------
--  DDL for Package RETAIL_ITEM_CENTRAL_SP3
--------------------------------------------------------


create or replace PACKAGE BODY              "RETAIL_ITEM_CENTRAL_SP3" 
as
  /*+
  Version History:
  v0.1, 11.11.2015, Ozgur Kuru (Ozkur2) (External - Oracle), Package shell created
  v0.9  20.11.2015, Ozgur Kuru (Ozkur2) (External - Oracle), Business logic coded
  v1.0  26.11.2015, Ozgur Kuru (Ozkur2) (External - Oracle), First version finalized.
  v1.1  12.05.2016, GuruSankar P (gurup) (External - Capgemini), Changed base Actual Run Date Filter constraint to include SRC_DELETE_DATE filter as per IR 10309 
  v2.0 17.06.2016, Ozgur Kuru (Ozkur2) (External - Oracle). p_ri_complex_tab was defined as the only output parameter for the GET_RETAIL_ITEM procedure. 
  v2.1 20.06.2016, Ozgur Kuru (Ozkur2) (External - Oracle). populate_colltext_table procedure added. Also other clean-up activities undertaken.
  v2.2 27.06.2016, Ozgur Kuru (Ozkur2) (External - Oracle). Except for Class and Item filters, all filters were turned to be optional.
  v3.0 12.07.2016,  Ozgur Kuru (Ozkur2) (External - Oracle). Business logic updated to compose final response.
  v3.1 15.08.2016, Ozgur Kuru (Ozkur2) (External - Oracle). Clob handling mechanism changed to remove limitation incurred regarding big size input parameter (eg 100s of item numbers)
*/
    -- Ozkur2@04.03.2016: Overwrite debug level here if needed: 
    --                    0:  No log
    --                    1:  Log only main Success/Failure messages for beginning and end 
    --                    2:  Log messages for each step 
    --                    3:  In addition to 2 log also each SQL script generated.
    --////////////////////////////////////////////////
    v_debug_level  number:=3;  
    --///////////////////////////////////////////////
    v_step_seq number:=0;

procedure GET_RETAIL_ITEM(
    p_ri_class_tab            in  ri_class_typn,
	p_ri_item_tab             in  ri_item_typn,
    p_ri_price_type_tab       in  ri_price_type_typn default ri_price_type_typn(ri_price_type_typ(null)),
    p_ri_tax_type_tab         in ri_tax_type_typn default ri_tax_type_typn(ri_tax_type_typ(null)),
    p_ri_timeconst_filter_tab in  ri_timeconst_filter_typ default ri_timeconst_filter_typ (null,null,null,null,null,null),
    p_ri_resp_filter_tab in ri_resp_filter_typn default ri_resp_filter_typn(ri_resp_filter_typ(null)),
    p_ri_complex_tab out ri_complex_typn
 )
is
    v_curr_seqno number;
    v_curr_ts timestamp;
-- Ozkur2@12.08.2016:  Change Nr.1 as part of v3.1: Changed v_insert_body and v_sql_text to clob. Added a new clob variable: v_insert_sql.
    v_insert_body clob;
	 v_insert_sql clob;
    v_sql_text clob;
    v_cnt_sql_text long;
    v_sql_text_tax long;
    v_collection_text long;
    v_collection_nested_text long;
    v_collection_nested_textgen long;
    v_collection_nested_text_tax long;
    v_collection_nested_textgen_tx long;
    v_system_message long;
    v_pricetype_addon long :='';
    v_debug_level number;
    v_cnt number;
    v_step_seq number:=0;
    v_session_info varchar2(500);
    v_take_prevprice number := 0;
    v_temptable_def long;
    v_distinct_flag boolean:=false;
    v_only_changed_price boolean:=false;
    v_column_list varchar2(1000);
    v_ncolumn_list varchar2(1000);
    v_column_list_tax varchar2(1000);
    v_ncolumn_list_tax varchar2(1000);
    v_dummy_date date:=to_date('01.01.1990 00:00:00','DD.MM.YYYY HH24:MI:SS');
    v_collection_text_tax long;
    v_temptable_def_tax long;
    v_taxtype_addon long :='';
    v_tax_resp_filter varchar2(1) :='N';
    v_price_resp_filter varchar2(1) :='N';
    v_unq_grp_cnt number:=0;
    v_next_unq_grp_id number:=0;
    v_tax_grp_cnt number:=0;
    v_price_grp_cnt number:=0;
    v_cnt_complex number:=1;
    v_unq_grp_id number:=0;
    v_tot_price_cnt number:=0;
    v_tot_tax_cnt number:=0;
    v_source_ind varchar2(20);
    v_class_type ri_class_filter.class_type%type;
    v_class_unit_code ri_class_filter.class_unit_code%type;
    v_class_unit_type ri_class_filter.class_unit_type%type;
    v_item_type ri_item_key_filter.item_type%type;
    v_item_no ri_item_key_filter.item_no%type;
    p_ri_price_t_tab ri_price_t_typn;
    p_ri_price_tab    ri_price_typn;
    p_ri_tax_t_tab    ri_tax_t_typn;
    p_ri_tax_tab      ri_tax_typn;
    no_app_data_found exception;
    c_sqltext_refcur SYS_REFCURSOR;

begin

      v_step_seq := 0;
      
      -- Ozkur2@19.11.2015: Overwrite debug level here if needed: 
      --                    0:  No log
      --                    1:  Log only main Success/Failure messages for beginning and end 
      --                    2:  Log messages for each step 
      --                    3:  In addition to 2 log also each SQL script generated.
      
      --////////////////////////////////////////////////
      v_debug_level :=3;  
      --///////////////////////////////////////////////
      
      if v_debug_level in (1,2)
             
						select sys_context('USERENV','IP_ADDRESS')  into v_session_info  from dual;
		elsif v_debug_level = 3
				then 
						select 'IP Address: '||sys_context('USERENV','IP_ADDRESS')||' / OS User (Client): '||sys_context('USERENV','OS_USER')||' / Calling Program: '||sys_context('USERENV','MODULE')   into v_session_info  from dual;
      end if;
      
      -- Ozkur2@19.11.2015: Design requirement Nr 1: Retrieve a new Sequence No. and Sysdate
		v_curr_seqno := seq_iip_get_retail_item_prc.nextval;
		v_curr_ts := to_timestamp(sysdate);
      -- Ozkur2@19.11.2015: Initiating collections.
		p_ri_price_t_tab := ri_price_t_typn();
		p_ri_price_tab := ri_price_typn();
		p_ri_tax_t_tab := ri_tax_t_typn();
		p_ri_tax_tab := ri_tax_typn();
		p_ri_complex_tab := ri_complex_typn();

      --Ozkur2@19.11.2015: Logging block
      if v_debug_level >= 1
        then 
              v_step_seq := v_step_seq + 1;
              v_system_message := 'RETAIL_ITEMS_CENTRAL_sp3 started.';
              log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
      end if;

      -- Ozkur2@19.11.2015: Design requirement Nr 2: Store all records for RI_CLASS_TYP [] under RI_CLASS_FILTER. 
		-- Purpose of this loop: Preparation of the dynamic sql stament for a multiple insert operation
		-- Ozkur2@12.08.2016:  Change Nr.2 as part of v3.1: Changed v_insert_body and v_sql_text to clob. Added a new clob variable: v_insert_sql. Bug fix for max 32K variable problem. 
		 dbms_lob.createTemporary( v_insert_body, TRUE );
		 dbms_lob.createTemporary( v_insert_sql, TRUE );
        for i in p_ri_class_tab.first .. p_ri_class_tab.last
				
					dbms_lob.Append( v_insert_body, ' into ri_class_filter (seq_no,insert_ts,class_type,class_unit_type,class_unit_code) values 
																																		( '||v_curr_seqno||',
																																		'''||v_curr_ts||''',
																																		'''||p_ri_class_tab(i).class_type||''',
																																		'''||p_ri_class_tab(i).class_unit_type||''',
																																		'''||p_ri_class_tab(i).class_unit_code||''')'
																		 );
					end loop; 
      -- Ozkur2@19.11.2015:  Design requirement Nr 3: Store all records for RI_ITEM_TYP [] under RI_ITEM_KEY_FILTER
      --  Purpose of this loop: Further preparation of the dynamic sql stament for a multiple insert operation

      if p_ri_item_tab is not null
          then
                for i in p_ri_item_tab.first .. p_ri_item_tab.last
                loop
					     dbms_lob.Append( v_insert_body, ' into ri_item_key_filter (seq_no,insert_ts,item_type,item_no) values 
																																		  (' ||v_curr_seqno||',
																																		  '''||v_curr_ts||''',
																																		  '''||p_ri_item_tab(i).item_type||''',
																																		  '''||p_ri_item_tab(i).item_no||''')'
																				);
			  end loop;
      end if; --if p_ri_item_tab is not null
		-- Ozkur2@12.08.2016:  Change Nr.2 as part of v3.1 ends here.
      
      -- Ozkur2@19.11.2015:  Finalization and run of the dynamic sql stament for a multiple insert operation
      v_insert_sql := 'insert all '||v_insert_body||' select * from dual ';
     

      --Ozkur2@19.11.2015: Logging block
      if v_debug_level = 2
          then    
              v_step_seq := v_step_seq + 1;
              v_system_message := 'INSERT ALL statement is generated.';
      elsif v_debug_level = 3
          then 
              v_step_seq := v_step_seq + 1;
              v_system_message := 'INSERT ALL statement to populate RI_CLASS_FILTER and RI_ITEM_KEY_FILTER tables is generated.';
      end if;
      log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
      
       execute immediate (v_insert_sql);
      
      -- Ozkur2@19.11.2015: Logging block
      if v_debug_level = 2
				then    
						v_step_seq := v_step_seq + 1;
						v_system_message := 'INSERT ALL statement is successfully run.';
      elsif v_debug_level = 3
				then 
						v_step_seq := v_step_seq + 1;
						v_system_message := 'INSERT ALL statement to populate RI_CLASS_FILTER and RI_ITEM_KEY_FILTER tables is run: '||sql%rowcount||' rows inserted';
      end if;
      log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
      
      -- Ozkur2@15.06.2016: Using a temporary table to accommodate cartesien product for ITEM and CLASS parameters to ensure consistent unq_grp_id across application.
      -- Development Consideration:  This code piece needs to be moved to SAPP_CODE_CONSTRUCTS table.
      v_sql_text:='insert into sapp_temp_param_cartesian (seq_no,insert_ts,class_type,class_unit_type,class_unit_code,item_type,item_no,unq_grp_id)
																																 (select i.seq_no,i.insert_ts,c.class_type,c.class_unit_type,c.class_unit_code,i.item_type,i.item_no, 
																																  dense_rank() over (order by c.class_type,c.class_unit_code,c.class_unit_type,i.item_type,i.item_no) unq_grp_id 
																																					 from ri_item_key_filter i, ri_class_filter c
																																							where i.seq_no =c.seq_no
																																							and  i.seq_no ='||v_curr_seqno||')';
      
      
		execute immediate (v_sql_text);
     -- commit;
      --log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);
       
      -- Ozkur2@19.11.2015:  Design requirement Nr 4: Select records from CEM_RI_PRICE_V2 by performing inner joins with  RI_CLASS_FILTER, RI_ITEM_KEY_FILTER tables
      -- Ozkur2@19.11.2015: Starting with preparation of select query. Code parts to construct the query reside in SAPP_CODE_CONSTRUCTS table.

      select 
					cv_column_list, 
					cv_ncolumn_list, 
					-- OZKUR2 03.06.2016
					-- cv_collection_text,
					cv_ncolumn_list||cv_temptable_def||v_curr_seqno
      into 
            v_column_list,
            v_ncolumn_list, 
				--  v_collection_text,
            v_temptable_def
      from sapp_code_constructs 
					where sysdate between valid_from_date and valid_to_date
					and QUERY_TYPE='PRICE';
      
      select 
            cv_column_list, 
            cv_ncolumn_list, 
            -- OZKUR2 03.06.2016
				-- cv_collection_text,
            cv_ncolumn_list||cv_temptable_def||v_curr_seqno
      into 
            v_column_list_tax,
            v_ncolumn_list_tax, 
				--  v_collection_text_tax,
            v_temptable_def_tax
		from sapp_code_constructs 
					where sysdate between valid_from_date and valid_to_date
					and QUERY_TYPE='TAX';
					
      -- Ozkur2@19.11.2015:  Preparing Price Type filter add-on to be added at the end of temporary table definition
		
      v_pricetype_addon := 'and p.retail_price_type in (''';
      
      if p_ri_price_type_tab is not null
            then
                  for i in p_ri_price_type_tab.first .. p_ri_price_type_tab.last
                    
                         v_pricetype_addon   := v_pricetype_addon||p_ri_price_type_tab(i).price_type||'''';                      
								 if i = p_ri_price_type_tab.count 
										then
													v_pricetype_addon := v_pricetype_addon||')';
										else 
													v_pricetype_addon := v_pricetype_addon||',''';
								 end if; --i = p_ri_price_type_tab.count 
                    end loop;
      end if; --p_ri_price_type_tab is not null
      -- Ozkur2@27.06.2016: To handle the case where the price filter is provided empty, we do the following:
      if v_pricetype_addon= 'and p.retail_price_type in ('''
          then
              v_pricetype_addon:= 'and p.retail_price_type=p.retail_price_type';
      end if;      --v_pricetype_addon= 'and p.retail_price_type in ('''
        
      v_taxtype_addon := 'and p.tax_type in (''';
      
      if p_ri_tax_type_tab is not null
				then 
						for i in p_ri_tax_type_tab.first .. p_ri_tax_type_tab.last
							loop
									v_taxtype_addon   := v_taxtype_addon||p_ri_tax_type_tab(i).tax_type||'''';											  
									if i = p_ri_tax_type_tab.count 
										then
												v_taxtype_addon := v_taxtype_addon||')';
										else 
												v_taxtype_addon := v_taxtype_addon||',''';
								end if;
                end loop;
      end if; --p_ri_tax_type_tab is not null
      
      -- Ozkur2@27.06.2016: To handle the case where the tax filter is provided empty, we do the following:
      
      if v_taxtype_addon= 'and p.tax_type in ('''
				then
						v_taxtype_addon:= 'and p.tax_type=p.tax_type';
      end if;      
      
      
      -- ASGUP Response Filter starts  
      if p_ri_resp_filter_tab is not null
			then
					for i in p_ri_resp_filter_tab.first .. p_ri_resp_filter_tab.last
							loop
									if  p_ri_resp_filter_tab(i).filter_type =  'RetailItemPrice'    
										then
												v_price_resp_filter := 'Y';
									end if; --p_ri_resp_filter_tab(i).filter_type =  'RetailItemPrice'    
             
									if  p_ri_resp_filter_tab(i).filter_type =  'TaxInfo'
										then
												v_tax_resp_filter := 'Y';
									end if; --p_ri_resp_filter_tab(i).filter_type =  'TaxInfo'
							end loop;
		end if; --p_ri_resp_filter_tab is not null
      --ASGUP Response Filter ends
      
      -- Ozkur2@19.11.2015:  Preparing Price Type filter add-on is added here:
          v_temptable_def := v_temptable_def||'
                            '||v_pricetype_addon||' ';
      -- Asgup@19.11.2015:  Preparing Tax Type filter add-on is added here:
          v_temptable_def_tax := v_temptable_def_tax||'
                            '||v_taxtype_addon||' ';
                            
      -- Ozkur2@19.11.2015:  We continue structuring our temporary table definition by adding necessary date filters: Next few IF statements are for arranging date filters for Price.
		-- Ozkur2@19.11.2015:  Design requirement Nr 5 a.k.a. "DeltaPriceFromLastRunDate": If RI_TIMECONST_FILTER_TYP.DELTA_LAST_RUNDT <> empty, select records from FROM CEM_RI_PRICE_V2 that fulfill:  WHERE  HUB_REG_DATE > [Input] OR  HUB_UPD_DATE > [Input] 
      if  RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.delta_last_rundt,'D') != v_dummy_date
				then
                v_temptable_def := v_temptable_def ||' 
                                          and (p.HUB_REG_DATE >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.delta_last_rundt)||' or p.HUB_UPD_DATE >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.delta_last_rundt)||')';        
       -- Ozkur2@19.11.2015:  Design requirement Nr 6 a.k.a. PreviousPriceFromDate: Perform Time constraint filtering
		elsif  nvl(p_ri_timeconst_filter_tab.prev_from_date,to_date('01.01.1900','DD.MM.YYYY')) != to_date('01.01.1900','DD.MM.YYYY')
				then
						 v_temptable_def := v_temptable_def||' 
													  and '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.prev_from_date)||' >= p.HUB_UPD_DATE
													  and ('||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_TO_DATE,'U'))||' >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.prev_from_date)||' OR VALID_TO_DTIME IS NULL)';
						 v_distinct_flag := true;
						 v_take_prevprice := 1;
					-- Ozkur2@19.11.2015: Enable this parameter if we want the procedure to return rows only in the case when the current price is different than the current price. Eliminates cases of duplications.         
               -- v_only_changed_price := true;
			-- Ozkur2@19.11.2015:  Design requirement Nr 7 a.k.a. ValidationDate: If RI_TIMECONST_FILTER_TYP.VALID_FROM_DATE <> empty OR RI_TIMECONST_FILTER_TYP.VALID_TO_DATE Perform Validity Filtering  
		elsif   (RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.valid_from_date ,'D') != v_dummy_date or RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.valid_to_date,'D') != v_dummy_date)
				then
						v_temptable_def := v_temptable_def ||' 
                                          and (p.HUB_REG_DATE >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_FROM_DATE,'D'))||' or p.HUB_UPD_DATE >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_FROM_DATE,'D'))||')
                                          and (p.HUB_REG_DATE <= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_TO_DATE,'U'))||' or p.HUB_UPD_DATE <= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_TO_DATE,'U'))||')';
						v_distinct_flag := true;
      -- Ozkur2@23.11.2015: Design requirement Nr 8 a.k.a "ActualPriceDate": If RI_ TIMECONST_FILTER_TYP.ACTUAL_DATE Perform Actual Date Filtering
	 elsif RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.actual_date ,'D') != v_dummy_date
				then
						v_temptable_def := v_temptable_def||'
                                      and p.valid_from_dtime <= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_date)||' 
                                      and (p.valid_to_dtime  >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_date)||' or p.valid_to_dtime is null)
                                      and p.SRC_DEL_DATE IS NULL';
      -- Ozkur2@19.11.2015:  Design requirement Nr 9 a.k.a. ActualPriceSinceLastRunDate. Changes made to reflect output of meeting with Harri.
	elsif  RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.actual_slast_rundt,'D') != v_dummy_date
          then
                v_temptable_def := v_temptable_def||'
                                         and (sysdate between p.valid_from_dtime and nvl(p.valid_to_dtime,to_date(''31.12.2099'',''DD.MM.YYYY'')))
                                         and (p.HUB_REG_DATE >=p.valid_from_dtime or '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_slast_rundt)||' < nvl(p.valid_to_dtime,to_date(''31.12.2099'',''DD.MM.YYYY'')))';
                                    --  and ('||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_slast_rundt)||' <= nvl(p.HUB_UPD_DATE,p.HUB_REG_DATE)
                                    --  or  '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_slast_rundt)||' between p.valid_from_dtime and nvl(p.valid_to_dtime,to_date(''31.12.2099'',''DD.MM.YYYY'')))';
                v_distinct_flag := true;
	end if;
	
      --ASGUP Time Constraint Filter Start----
      
      
      -- Ozkur2@19.11.2015:  We continue structuring our temporary table definition by adding necessary date filters: Next few IF statements are for arranging date filters for Tax.
      
       -- Ozkur2@19.11.2015:  Design requirement Nr 5 a.k.a. "DeltaPriceFromLastRunDate": If RI_TIMECONST_FILTER_TYP.DELTA_LAST_RUNDT <> empty, select records from FROM CEM_RI_PRICE_V2 that fulfill: 
       --                                    WHERE  HUB_REG_DATE > [Input] OR  HUB_UPD_DATE > [Input] 
      
      if  RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.delta_last_rundt,'D') != v_dummy_date
				then
						 v_temptable_def_tax := v_temptable_def_tax ||' 
															and (p.IIP_INSERT_DATE >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.delta_last_rundt)||' or p.IIP_UPDATE_DATE >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.delta_last_rundt)||')';
       -- Ozkur2@19.11.2015:  Design requirement Nr 6 a.k.a. PreviousPriceFromDate: Perform Time constraint filtering
	 elsif  nvl(p_ri_timeconst_filter_tab.prev_from_date,to_date('01.01.1900','DD.MM.YYYY')) != to_date('01.01.1900','DD.MM.YYYY')
			 then
						 v_temptable_def_tax := v_temptable_def_tax||' 
													  and '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.prev_from_date)||' >= p.IIP_UPDATE_DATE
													  and ('||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_TO_DATE,'U'))||' >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.prev_from_date)||' OR VALID_TO_DTIME IS NULL)';
						 v_distinct_flag := true;
						 v_take_prevprice := 1;
						 -- Ozkur2@19.11.2015: Enable this parameter if we want the procedure to return rows only in the case when the current price is different than the current price. Eliminates cases of duplications.         
						 -- v_only_changed_price := true;
						-- Ozkur2@19.11.2015:  Design requirement Nr 7 a.k.a. ValidationDate: If RI_TIMECONST_FILTER_TYP.VALID_FROM_DATE <> empty OR RI_TIMECONST_FILTER_TYP.VALID_TO_DATE Perform Validity Filtering  
	 elsif   (RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.valid_from_date ,'D') != v_dummy_date or RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.valid_to_date,'D') != v_dummy_date)
          then
                  v_temptable_def_tax := v_temptable_def_tax ||' 
                                          and (p.IIP_INSERT_DATE >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_FROM_DATE,'D'))||' or p.IIP_UPDATE_DATE >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_FROM_DATE,'D'))||')
                                          and (p.IIP_INSERT_DATE <= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_TO_DATE,'U'))||' or p.IIP_UPDATE_DATE <= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.VALID_TO_DATE,'U'))||')';
						v_distinct_flag := true; 
      -- Ozkur2@23.11.2015: Design requirement Nr 8 a.k.a "ActualPriceDate": If RI_ TIMECONST_FILTER_TYP.ACTUAL_DATE Perform Actual Date Filtering
	 elsif RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.actual_date ,'D') != v_dummy_date
         then
						 v_temptable_def_tax := v_temptable_def_tax||'
													  and p.valid_from_dtime <= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_date)||' 
													  and (p.valid_to_dtime  >= '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_date)||' or p.valid_to_dtime is null)';
        
      -- Ozkur2@19.11.2015:  Design requirement Nr 9 a.k.a. ActualPriceSinceLastRunDate. Changes made to reflect output of meeting with Harri.
	 elsif  RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown(p_ri_timeconst_filter_tab.actual_slast_rundt,'D') != v_dummy_date
          then
                v_temptable_def_tax := v_temptable_def_tax||'
                                         and (sysdate between p.valid_from_dtime and nvl(p.valid_to_dtime,to_date(''31.12.2099'',''DD.MM.YYYY'')))
                                         and (p.IIP_INSERT_DATE >=p.valid_from_dtime or '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_slast_rundt)||' < nvl(p.valid_to_dtime,to_date(''31.12.2099'',''DD.MM.YYYY'')))';
                                    --  and ('||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_slast_rundt)||' <= nvl(p.HUB_UPD_DATE,p.HUB_REG_DATE)
                                    --  or  '||RETAIL_ITEM_CENTRAL_SP3.convert_date_string(p_ri_timeconst_filter_tab.actual_slast_rundt)||' between p.valid_from_dtime and nvl(p.valid_to_dtime,to_date(''31.12.2099'',''DD.MM.YYYY'')))';
                v_distinct_flag := true;
	 end if;
      -- ASGUP Time Constraint Filter Ends----
      
      -- Ozkur2@03.06.2015: Establish the statement that is needed to generate the nested collection text fror pricing information
      -- Ozkur2@03.06.2016 For that, as first step we use generic function created for this purpose:
		v_collection_nested_textgen:= generate_collection_query ('DS_PRODUCT','CEM_RI_PRICE_T_TYP');
      
      --Ozkur2@06.06.2016 - Using Global Temporary Table instead of  With ... clause to enable more flexibility in coming steps. This change involves;
      -- 1- Inserting new records into the already created SAPP_TEMP_TABLE
      -- 2- Reading from this table in coming steps.
      
      v_sql_text:='insert into SAPP_TEMP_TABLE ('||v_column_list||') select '||v_temptable_def;
      -- Ozkur2@06.06.2016: Logging block
      if v_debug_level = 2
				then   
						v_step_seq := v_step_seq + 1;
						v_system_message := 'Insert statement to populate SAPP_TEMP_TABLE is generated.';
      elsif v_debug_level = 3
				then 
						v_step_seq := v_step_seq + 1;
						v_system_message := 'Insert statement to populate SAPP_TEMP_TABLE is generated: '||v_sql_text;
      end if;
      log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
		
      execute immediate (v_sql_text);
      
      -- Ozkur2@06.06.2016: Logging block
      if v_debug_level = 2
				then    
						v_step_seq := v_step_seq + 1;
						v_system_message := 'Prepared insert statement successfully run: '||SQL%ROWCOUNT||' rows inserted.';
      elsif v_debug_level = 3
				then 
						v_step_seq := v_step_seq + 1;
						v_system_message := 'Prepared insert statement successfully run: '||SQL%ROWCOUNT||' rows inserted.';
      end if;
      log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
      
		if v_distinct_flag
				then 
						v_sql_text:= 'select '||v_collection_nested_textgen||' gentext ,unq_grp_id,nvl((lag(unq_grp_id) over (order by unq_grp_id)),0) p_unq_grp_id, nvl((lead(unq_grp_id) over (order by unq_grp_id)),0) n_unq_grp_id,seq_no from SAPP_TEMP_TABLE'||' where seq_no='||v_curr_seqno||' date_rank=1 order by unq_grp_id';
		else 
						v_sql_text:= 'select '||v_collection_nested_textgen||' gentext ,unq_grp_id,nvl((lag(unq_grp_id) over (order by unq_grp_id)),0) p_unq_grp_id, nvl((lead(unq_grp_id) over (order by unq_grp_id)),0) n_unq_grp_id, seq_no from SAPP_TEMP_TABLE'||' where seq_no='||v_curr_seqno||' order by unq_grp_id';
		end if;
      
      if v_only_changed_price
				then 
						v_sql_text:=v_sql_text||' and (LAST_PRICE_EXCL_TAX != PRICE_EXCL_TAX or LAST_PRICE_INCL_TAX != LAST_PRICE_INCL_TAX)';
      end if;
		
      -- Ozkur2@03.06.2016 Wrapper to reorganize return of multiple rows to comma separated single row
      populate_colltext_table(v_sql_text,'PRICE');
          
      --Ozkur2@03.06.2016: Logging block
		if v_debug_level > 1
				then 
						 v_step_seq := v_step_seq + 1;
						 v_system_message := 'Necessary collection texts for Price was successfully generated for all unq_grp_ids';
						 log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
		end if;

		--Ozkur2@06.06.2016: Now we are doing everything we did so far also for Tax:
      v_collection_nested_textgen_tx:= generate_collection_query ('DS_PRODUCT','CEM_RI_TAX_INFO_T_TYP');
      v_sql_text_tax:='insert into SAPP_TEMP_TABLE_TAX ('||v_column_list_tax||') select '||v_temptable_def_tax;
      
      --Ozkur2@06.06.2016: Logging block start
      if  v_debug_level = 2
				then    
							v_step_seq := v_step_seq + 1;
							v_system_message := 'Insert statement to populate SAPP_TEMP_TABLE_TAX is generated.';
      elsif v_debug_level = 3
            then 
							v_step_seq := v_step_seq + 1;
							v_system_message := 'Insert statement to populate SAPP_TEMP_TABLE_TAX is generated: '||v_sql_text_tax;
      end if;
      log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
      
      execute immediate (v_sql_text_tax);
      
		--Ozkur2@06.06.2016: Logging block start
      if v_debug_level = 2
            then    
							v_step_seq := v_step_seq + 1;
							v_system_message := 'Prepared insert statement successfully run: '||SQL%ROWCOUNT||' rows inserted.';
      elsif v_debug_level = 3
            then 
							v_step_seq := v_step_seq + 1;
							v_system_message := 'Prepared insert statement successfully run: '||SQL%ROWCOUNT||' rows inserted.';
      end if;
      log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);

      v_sql_text_tax:= 'select '||v_collection_nested_textgen_tx||' gentext, unq_grp_id, 
															nvl((lag(unq_grp_id) over (order by unq_grp_id)),0) p_unq_grp_id, 
															nvl((lead(unq_grp_id) over (order by unq_grp_id)),0) n_unq_grp_id, 
															seq_no 
																	from SAPP_TEMP_TABLE_TAX'||' 
																	where seq_no='||v_curr_seqno||''; 
																	
		-- Ozkur2@03.06.2016 Wrapper to reorganize return of multiple rows to comma separated single row
		 populate_colltext_table(v_sql_text_tax,'TAX');

		-- Ozkur2@03.06.2016: Logging block 
		v_step_seq := v_step_seq + 1;
		if v_debug_level > 1
				then 
                  v_system_message := 'Necessary collection texts for Tax was successfully generated for all unq_grp_ids';    
                  log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
		end if;

	-- Ozkur2@06.06.2016: Following loop's purpose is to finalize packaging of the output in desired nested format. 
	-- There is one loop, which incapsulates results coming from inner sections. Number of repeat for the the loop is v_unq_grp_cnt. 
	-- Inner sections operate as long is equal or smaller than v_tax_grp_cnt and v_price_grp_cnt respectively.
    v_sql_text:='select nvl(count(unq_grp_id),0) from sapp_temp_table where seq_no='||v_curr_seqno;
    execute immediate v_sql_text into v_tot_price_cnt;
    v_sql_text_tax:='select nvl(count(unq_grp_id),0) from sapp_temp_table_tax where seq_no='||v_curr_seqno;
    execute immediate v_sql_text_tax into v_tot_tax_cnt;
    
      if v_tot_price_cnt = 0 
             then
                  --Ozkur2@19.11.2015: Logging block start
                   v_step_seq := v_step_seq + 1;
                   v_system_message := 'No Pricing data found for given parameters';
                    log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                    --Ozkur2@19.11.2015: Logging block end
     elsif v_tot_tax_cnt = 0 
            then
                  --Ozkur2@19.11.2015: Logging block start
                   v_step_seq := v_step_seq + 1;
                   v_system_message := 'No Tax data found for given parameters';
                    log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                    --Ozkur2@19.11.2015: Logging block end
      end if;
                                

      v_sql_text:='select count(*) from  (select unq_grp_id from sapp_temp_table where seq_no='||v_curr_seqno||' and '''||v_price_resp_filter||'''=''Y'' 
                                union 
                          select unq_grp_id from sapp_temp_table_tax where seq_no='||v_curr_seqno||' and '''||v_tax_resp_filter||'''=''Y'' )';
      execute immediate v_sql_text into  v_unq_grp_cnt;
      
--Ozkur2@28.06.2016: This is the start of the big outer IF.. THEN .. END IF block. We get into it only there is data to be returned.
    if ((v_tot_price_cnt > 0 and v_price_resp_filter='Y') or (v_tot_tax_cnt > 0 and v_tax_resp_filter='Y'))
         then
            --Ozkur2@28.06.2016:  Extending collections.
            p_ri_complex_tab.EXTEND(v_unq_grp_cnt);
            --Ozkur2@28.06.2016: If there is no price or tax information at all (for any unq_grp_id), we are logging this case here.
            --Ozkur2@28.06.2016: Let's get started with the loop to go through all unq_grp_id's
            v_sql_text:='select unq_grp_id, source_ind, nvl(lead(unq_grp_id) over (order by unq_grp_id, source_ind),0)  next_unq_grp_id  from  (
                                            select unq_grp_id,''PRICE'' source_ind from sapp_temp_table where seq_no='||v_curr_seqno||' and '''||v_price_resp_filter||'''=''Y'' 
                                                    union 
                                            select unq_grp_id, ''TAX'' from sapp_temp_table_tax where seq_no='||v_curr_seqno||' and '''||v_tax_resp_filter||'''=''Y'' )
                                  order by unq_grp_id, source_ind';
            open c_sqltext_refcur for v_sql_text;
                  loop
                          fetch c_sqltext_refcur into v_unq_grp_id, v_source_ind, v_next_unq_grp_id;
                          exit when c_sqltext_refcur %notfound;       
                          
								 -- dbms_output.put_line (v_unq_grp_id||' - Type:'||v_source_ind||' - Price Filter:'||v_price_resp_filter||' - Tax Filter:'||v_tax_resp_filter||' - Next Group ID:'||v_next_unq_grp_id);
                          --Ozkur2@19.11.2015: Logging block start                                            
                           if v_debug_level > 2
                                then 
                                        v_step_seq := v_step_seq + 1;
                                        v_system_message := 'Processing '||v_cnt_complex||'. member of complex type. (Unq_Grp_ID: '||v_unq_grp_id||') for '||v_source_ind||'.';
                                        log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                          end if;
                        
                        
                          --Ozkur2@28.06.2016: Go into this section only if it is desired that PRICE information is returned.

                        if v_source_ind='PRICE' 
                           then    
                                --v_collection_text:='null';
                                if v_price_resp_filter = 'Y'
                                then           
                                           -- Check if any Pricing data exists for the current unique group id.
                                            v_sql_text:='select nvl(count(unq_grp_id),0) from sapp_temp_table where seq_no='||v_curr_seqno||' and unq_grp_id='||v_unq_grp_id;
                                            execute immediate v_sql_text into v_price_grp_cnt;
                                             if v_price_grp_cnt > 0
                                                   then                   
                                                             --   v_collection_text:='null';
                                                                p_ri_price_tab.EXTEND(v_price_grp_cnt);                                    
                                                                -- Ozkur2@14.06.2016: Inner section for PRICE information starting here.
                                                                v_sql_text:='select count(*) from sapp_temp_coll_text where QUERY_TYPE = ''PRICE'' and seq_no='||v_curr_seqno||' and unq_grp_id='||v_unq_grp_id;
                                                                execute immediate v_sql_text into v_cnt;
        
                                                                if v_cnt=0 
                                                                        then 
                                                                          v_collection_text:='null';
                                                                else --v_cnt=0 
                                                                          v_sql_text:='select collection_text from sapp_temp_coll_text where QUERY_TYPE = ''PRICE'' and seq_no='||v_curr_seqno||' and unq_grp_id='||v_unq_grp_id;
                                                                          execute immediate v_sql_text into v_collection_nested_text;
                                                                          v_collection_text:='ri_price_typ(seq_no,insert_ts,item_type,item_no, RI_PRICE_T_TYPN('||v_collection_nested_text||'),   class_type,class_unit_type,class_unit_code)';
                                                               end if; --v_cnt=0 
                    
                                                              if v_distinct_flag
                                                                        then 
                                                                            v_sql_text:= 'select '||replace(v_collection_text,'tv_take_prevprice',v_take_prevprice)||' from SAPP_TEMP_TABLE'||' where seq_no='||v_curr_seqno||' and gen_uk=1and date_rank=1';
                                                              else --v_distinct_flag
                                                                            v_sql_text:= ' select '||replace(v_collection_text,'tv_take_prevprice',v_take_prevprice)||' from SAPP_TEMP_TABLE where seq_no='||v_curr_seqno||' and gen_uk=1';
                                                              end if; --v_distinct_flag    
        
                                                              if v_only_changed_price
                                                                        then 
                                                                            v_sql_text:=v_sql_text||' and (LAST_PRICE_EXCL_TAX != PRICE_EXCL_TAX or LAST_PRICE_INCL_TAX != LAST_PRICE_INCL_TAX)';
                                                              end if;
                                                              
                                                              v_sql_text:=v_sql_text||' and unq_grp_id='||v_unq_grp_id;
                                                                    
                                                              --Ozkur2@19.11.2015: Logging block start
                                                              
                                                              if v_debug_level > 2
                                                                      then 
                                                                          v_step_seq := v_step_seq + 1;
                                                                          v_system_message := 'Code fragment to populate price list to be placed in '||v_cnt_complex||'. member of complex type is generated: '||v_collection_text;
                                                                          log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                                              end if;
                                                  else --v_price_grp_cnt>0
                                                           --Ozkur2@19.11.2015: Logging block start
                                                          v_step_seq := v_step_seq + 1;
                                                          v_system_message := 'No Pricing data found for Unq_Grp_ID: '||v_unq_grp_id;
                                                          log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                                          --v_collection_text:='null';
                                              --Ozkur2@19.11.2015: Logging block end
                                                end if;--v_price_grp_cnt>0
                                  else --v_price_resp_filter = 'Y'
                                        if v_debug_level > 2
                                           then 
                                              v_step_seq := v_step_seq + 1;
                                              v_system_message := 'Price filter disabled for this query. Continuing.';
                                              log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                              --v_collection_text:='null';
                                      end if;
                                      --Ozkur2@11.07.2016: We don't want to "continue". We want to move to Tax section first. Hence commented "continue" statement below.
                                      --continue;
                              end if; --v_source_ind='PRICE' 
                    -- Ozkur2@28.06.2016: Let's deal with Tax as we dealt with Price above.
                    elsif v_source_ind='TAX'  --v_source_ind='PRICE' 
                          then
                                --v_collection_text_tax:='null';
                                if v_tax_resp_filter = 'Y'
                                  then           
                                      -- Check if any Tax data exists for the current unique group id.
                                      v_sql_text_tax:='select nvl(count(unq_grp_id),0) from sapp_temp_table_tax where seq_no='||v_curr_seqno||' and unq_grp_id='||v_unq_grp_id;
                                      execute immediate v_sql_text_tax into v_tax_grp_cnt;
                                      if v_tax_grp_cnt > 0
                                           then              
                                                          -- v_collection_text_tax:='null';
                                                            p_ri_tax_tab.EXTEND(v_tax_grp_cnt);
                                                            v_sql_text:='select count(*) from sapp_temp_coll_text where QUERY_TYPE = ''TAX'' and seq_no='||v_curr_seqno||' and unq_grp_id='||v_unq_grp_id;
                                                            execute immediate v_sql_text into v_cnt;
                                                            if v_cnt=0 
                                                                then 
                                                                      v_collection_text_tax:='null';
                                                            else
                                                                      v_sql_text_tax:='select collection_text from sapp_temp_coll_text where QUERY_TYPE = ''TAX'' and seq_no='||v_curr_seqno||' and unq_grp_id='||v_unq_grp_id;
                                                                      execute immediate v_sql_text_tax into v_collection_nested_text_tax;
                                                                      v_collection_text_tax:='ri_tax_typ(seq_no,insert_ts,item_type,item_no, RI_TAX_T_TYPN('||v_collection_nested_text_tax||'),   class_type,class_unit_type,class_unit_code)';
                                                             end if; --v_cnt=0 
                                                        /*+    v_sql_text_tax:= ' select '||v_collection_text_tax||' from SAPP_TEMP_TABLE_TAX where seq_no='||v_curr_seqno||' and gen_uk=1';
                                                            v_sql_text_tax:=v_sql_text_tax||' and unq_grp_id='||v_unq_grp_id;*/
                                         
                                                          --Ozkur2@19.11.2015: Logging block start
                                                            if v_debug_level > 2
                                                                then 
                                                                      v_step_seq := v_step_seq + 1;
                                                                      v_system_message := 'Code fragment to populate tax to be placed in '||v_cnt_complex||'. member of complex type is generated: '||v_collection_text_tax;
                                                                      log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                                             end if;
                                                            --Ozkur2@19.11.2015: Logging block end
                                        else --v_tax_grp_cnt>0
                                                   --Ozkur2@19.11.2015: Logging block start
                                                  v_step_seq := v_step_seq + 1;
                                                  v_system_message := 'No Tax data found for Unq_Grp_ID: '||v_unq_grp_id;
                                                  log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                                  --v_collection_text_tax:='null';
                                      --Ozkur2@19.11.2015: Logging block end
                                        end if;--v_tax_grp_cnt>0
                            else --v_tax_resp_filter = 'Y'
                                        if v_debug_level > 2
                                           then 
                                              v_step_seq := v_step_seq + 1;
                                              v_system_message := 'Tax filter disabled for this query. Continuing.';
                                              log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                              --v_collection_text_tax:='null';
                                      end if;
                          end if;  -- v_tax_resp_filter = 'Y'          
              end if;  --elsif v_source_ind='TAX' 

                 if  (v_collection_text is not null or v_collection_text_tax is not null)
                      then
                          --Ozkur2@11.07.2016: It is here we are formulating complex output. 
                          if v_next_unq_grp_id <> v_unq_grp_id
                                then 
                                      v_collection_text_tax:=nvl(v_collection_text_tax,'null');
                                      v_collection_text:=nvl(v_collection_text,'null');
                                      v_sql_text:='(
                                                              ( select seq_no,unq_grp_id,insert_ts,class_type,class_unit_code,class_unit_type,item_type,item_no from SAPP_TEMP_TABLE_TAX
                                                                                  union
                                                                select seq_no,unq_grp_id,insert_ts,class_type,class_unit_code,class_unit_type,item_type,item_no from SAPP_TEMP_TABLE )
                                                            )
                                                               where seq_no='||v_curr_seqno||' and unq_grp_id='||v_unq_grp_id;
                                                    
                                      --Ozkur2@28.06.2016: Since we have populated Tax and Price lists, we can build members of our complex list.
                                    v_sql_text:='select ri_complex_typ (class_type, class_unit_type, class_unit_code, item_type, item_no,'||v_collection_text||', '||v_collection_text_tax||') from'||v_sql_text ;
                                    --v_sql_text:=replace (v_sql_text,',,',',null,');
                                      --Ozkur2@06.06.2016: Logging block start
                                      if v_debug_level = 2        
                                          then   
                                                v_step_seq := v_step_seq + 1;
                                                v_system_message := 'Select statement to populate '||v_cnt_complex||'. member RI_COMPLEX_TYP is generated. (Unq_Grp_Id= '||v_unq_grp_id||')';
                                                 log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                       elsif v_debug_level >= 3
                                            then
                                                  v_step_seq := v_step_seq + 1;
                                                  v_system_message := 'Select statement to populate '||v_cnt_complex||'. member RI_COMPLEX_TYP is generated. (Unq_Grp_Id= '||v_unq_grp_id||'): '||v_sql_text;
                                                  log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                        end if;
                                        --Ozkur2@06.06.2015: Logging block end        
                                      execute immediate v_sql_text into p_ri_complex_tab(v_cnt_complex);     
                    
                                      --Ozkur2@06.06.2016: Logging block start                             
                                      if v_debug_level >= 2
                                        then    
                                                  v_step_seq := v_step_seq + 1;
                                                  v_system_message := 'Select statement to populate '||v_cnt_complex||'. member RI_COMPLEX_TYP is successfully run. (Unq_Grp_Id= '||v_unq_grp_id||')';
                                                  log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                                      end if;
                                      --Ozkur2@06.06.2016: Logging block end
                                     v_cnt_complex:=v_cnt_complex+1;
                                     v_collection_text_tax:=null;
                                     v_collection_text:=null;
                        end if; --v_next_unq_grp_id <> v_unq_grp_id
                else 
                          --Ozkur2@19.11.2015: Logging block start
                          if v_debug_level >= 1
                              then
                                  v_step_seq := v_step_seq + 1;
                                  v_system_message := 'No necessity to build a response. (Unq_Grp_Id= '||v_unq_grp_id||')  Either no data or filter(s) deselected. Continuing.';
                                  log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                        end if;
                end if; -- (v_collection_text is not null and v_collection_text_tax is not null)
                end loop;
            close c_sqltext_refcur;
                
                --Ozkur2@19.11.2015: Logging block start
                 if v_debug_level >= 1
                      then
                            v_cnt_complex:=v_cnt_complex-1;
                            v_step_seq := v_step_seq + 1;
                            v_system_message := 'Retail information successfully queried: '||v_cnt_complex||' rows returned.';
                             log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                end if;
              --Ozkur2@19.11.2015: Logging block end
          else --v_unq_grp_cnt > 0
                --select ri_complex_typ (null, null, null, null,null, null, null) into p_ri_complex_tab(i) from dual;
                --Ozkur2@19.11.2015: Logging block start
                 if v_debug_level >= 1
                      then
                            v_step_seq := v_step_seq + 1;
                            v_system_message := 'No necessity to build a response. Either no data or filter(s) deselected. Exiting.';
                            log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                end if;
                null;
          end if;--v_unq_grp_cnt > 0
            
-- Ozkur2@20.06.2016 - Exception is disabled in order to escalate the error to OSB layer. When debugging this procedure, enable this section by uncommenting.        
/*exception 
    when no_app_data_found 
          then
                    null;
    when others
          then 
                    v_step_seq := (v_step_seq + 1)*-1;
                    v_system_message := 'An error was encountered - '||sqlcode||' -ERROR- '||sqlerrm;
                    log_sapp_messages_sp1.log_sapp_messages(v_curr_seqno,v_debug_level,'S', v_system_message,sysdate,v_step_seq,v_session_info);  
                    dbms_output.put_line (v_system_message);*/
end;

procedure populate_colltext_table (p_sqltext long, p_querytype varchar2)
is
c_colltext_refcur SYS_REFCURSOR;
v_gentext clob;
v_gentext_part varchar2(4000);
v_pgentext_part varchar2(4000);
v_curr_seqno number;
v_unq_grp_id number;
v_punq_grp_id number;
v_nunq_grp_id number;

begin

open c_colltext_refcur for p_sqltext;
loop

     
       fetch c_colltext_refcur into v_gentext_part, v_unq_grp_id,v_punq_grp_id, v_nunq_grp_id,v_curr_seqno;
       exit when c_colltext_refcur %notfound;
       
      if v_unq_grp_id != v_punq_grp_id
          then
              if v_gentext is not null
                  then
                        insert into sapp_temp_coll_text (collection_text, unq_grp_id, seq_no,QUERY_TYPE)  values (v_gentext,v_punq_grp_id,v_curr_seqno,p_querytype);
              end if;
              v_gentext:=v_gentext_part;
       end if;
      
      if v_unq_grp_id = v_punq_grp_id
          then
              v_gentext:=v_gentext||', '||v_gentext_part;      
       end if;          
end loop;


      if (v_unq_grp_id = v_punq_grp_id or v_nunq_grp_id  = 0)
          then
                  insert into sapp_temp_coll_text (collection_text, unq_grp_id, seq_no,QUERY_TYPE)  values (v_gentext,v_unq_grp_id,v_curr_seqno,p_querytype);
      end if;
close c_colltext_refcur;
--commit; 
end;


function convert_date_string (p_input_date in date) return varchar2
is
v_return_string varchar2(500);
begin

		if (p_input_date is null or p_input_date = to_date ('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'))
				then
						v_return_string := 'null';
		else 
						v_return_string := 'to_date('''||to_char(p_input_date,'DD.MM.YYYY HH24:MI:SS')||''',''DD.MM.YYYY HH24:MI:SS'')';
		end if;
		
		return v_return_string;
		
exception 
	 when others then
				null;
end;

function nvl_roll_updown (p_input_date in date,p_direction in varchar2) return date
is
v_return_date date;
v_rollup_date date := to_date ('31.12.2099 23:59:59','DD.MM.YYYY HH24:MI:SS');
v_rolldown_date date := to_date ('01.01.1990 00:00:00','DD.MM.YYYY HH24:MI:SS');
v_null_date date:=to_date ('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');
begin

		if p_input_date is null
				then
					 if p_direction = 'U'
							then
									v_return_date := v_rollup_date;
					elsif p_direction = 'D'
							then
									v_return_date := v_rolldown_date;
					elsif p_direction = 'N'
							then
									v_return_date := v_null_date;
									--v_return_date := null;
					end if;
		else
					v_return_date := p_input_date;
		end if;
		return v_return_date;
		
end;

function generate_collection_query (p_collection_owner varchar2, p_collection_name varchar2) return clob
is
v_return_string clob;
v_system_message long;
v_session_info varchar2(500);
v_module_name varchar2 (200);
v_return_string_part varchar2 (500);
v_attr_name all_type_attrs.attr_name%type;
v_attr_type all_type_attrs.attr_type_name%type;
v_tot_rows number;

cursor c_collection_attrs is 
     select attr_name,attr_type_name,count(*) over () tot_rows 		
				from all_type_attrs
						where type_name=p_collection_name
						 and owner=p_collection_owner
								 order by attr_no;
      
begin
		select return_session_info(v_debug_level) into v_session_info from dual;
		--v_module_name:=who_am_i;
		if v_debug_level  >= 4
				then 
							v_system_message := v_module_name||' run.';
		end if;
		
		v_return_string:=''''||p_collection_name||'(''||';
		open c_collection_attrs;
				loop
						 fetch c_collection_attrs into v_attr_name, v_attr_type, v_tot_rows;
						 exit when c_collection_attrs %notfound;
						  if v_attr_type in ('CHAR','VARCHAR2')
									then
											 v_return_string_part := '''''''''||'||v_attr_name||
																  case when c_collection_attrs%rowcount = v_tot_rows then '||'''''''')'  else '||'''''',''||'   end;
						  elsif v_attr_type in ('NUMBER')
									then
																	v_return_string_part := 'nvl('||v_attr_name||',0)'||
																	case when c_collection_attrs%rowcount = v_tot_rows then ')'  else '||'',''||' end;
						  elsif v_attr_type in ('DATE')
									then
												  v_return_string_part := 'RETAIL_ITEM_CENTRAL_SP3.convert_date_string (RETAIL_ITEM_CENTRAL_SP3.nvl_roll_updown('||v_attr_name||',''N''))'||
																	 case when c_collection_attrs%rowcount = v_tot_rows then '||'')''' else '||'',''||' end;
							end if;	
							v_return_string:=v_return_string||chr(13)||chr(10)||v_return_string_part;
				end loop;
		  close c_collection_attrs;
		 return v_return_string;
end;


function return_session_info (p_debug_level number) return varchar2
is
v_system_message long;
v_session_info varchar2(500);
v_module_name varchar2 (200);


begin

		if p_debug_level in (1,2)
				then 
						select sys_context('USERENV','IP_ADDRESS')  into v_session_info  from dual;
		elsif p_debug_level = 3
				then 
						select 'IP Address: '||sys_context('USERENV','IP_ADDRESS')||' / OS User (Client): '||sys_context('USERENV','OS_USER')||' / Calling Program: '||sys_context('USERENV','MODULE')   into v_session_info  from dual;
		end if;
		return v_session_info;
		
end;

function who_am_i return varchar2
is
   l_owner        varchar2(30);
   l_name      varchar2(30);
   l_lineno    number;
   l_type      varchar2(30);
begin
		who_called_me( l_owner, l_name, l_lineno, l_type );
		return l_owner || '.' || l_name;
end;

procedure who_called_me( owner      out varchar2,
                        name       out varchar2,
                        lineno     out number,
                        caller_t   out varchar2 )
as
   call_stack  varchar2(4096) default dbms_utility.format_call_stack;
   n           number;
   found_stack BOOLEAN default FALSE;
   line        varchar2(255);
   cnt         number := 0;
begin
--
   loop
       n := instr( call_stack, chr(10) );
       exit when ( cnt = 3 or n is NULL or n = 0 );
--
       line := substr( call_stack, 1, n-1 );
       call_stack := substr( call_stack, n+1 );
--
       if ( NOT found_stack ) then
           if ( line like '%handle%number%name%' ) then
               found_stack := TRUE;
           end if;
       else
           cnt := cnt + 1;
           -- cnt = 1 is ME
           -- cnt = 2 is MY Caller
           -- cnt = 3 is Their Caller
           if ( cnt = 3 ) then
               lineno := to_number(substr( line, 13, 6 ));
               line   := substr( line, 21 );
               if ( line like 'pr%' ) then
                   n := length( 'procedure ' );
               elsif ( line like 'fun%' ) then
                   n := length( 'function ' );
               elsif ( line like 'package body%' ) then
                   n := length( 'package body ' );
               elsif ( line like 'pack%' ) then
                   n := length( 'package ' );
               elsif ( line like 'anonymous%' ) then
                   n := length( 'anonymous block ' );
               else
                   n := null;
               end if;
               if ( n is not null ) then
                  caller_t := ltrim(rtrim(upper(substr( line, 1, n-1 ))));
               else
                  caller_t := 'TRIGGER';
               end if;

               line := substr( line, nvl(n,1) );
               n := instr( line, '.' );
               owner := ltrim(rtrim(substr( line, 1, n-1 )));
               name  := ltrim(rtrim(substr( line, n+1 )));
           end if;
       end if;
   end loop;
end;

end RETAIL_ITEM_CENTRAL_SP3;


/