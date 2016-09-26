import pynza
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import subprocess
import re

NZ_CONNECTION_STRING = "dsn=mi_sdm_ds"

DXCG_COLUMNS = [('ALL_COLUMNS', 'VARCHAR(1000)')]
DXCG_MEMBER_INPUT_TABLE_SCRIPT = os.path.dirname(os.path.abspath(__file__)) + r'/DXCG_MEMBER_INPUT_TABLE_SCRIPT.sql'
DXCG_DIAGNOSIS_INPUT_TABLE_SCRIPT = os.path.dirname(os.path.abspath(__file__)) + r'/DXCG_DIAGNOSIS_INPUT_TABLE_SCRIPT.sql'
DXCG_OUTPUT_TABLE_SCRIPT = os.path.dirname(os.path.abspath(__file__)) + r'/DXCG_OUTPUT_TABLE_SCRIPT.sql'
DXCG_APPEND_OUTPUT_TABLE_SCRIPT = os.path.dirname(os.path.abspath(__file__)) + r'/DXCG_APPEND_OUTPUT_TABLE_SCRIPT.sql'
PATH = os.path.dirname(os.path.abspath(__file__))

    ##########        Phase 1: Build Input table
def build_dxcg_input(sdm_batch, current_iteration, source_iteration, current_iteration_date,
                     input_member_file, input_diagnosis_file,start_date, end_date, run_out, run_out_date):

    logger = logging.getLogger('DxCG_Build_Input_Table')
    logger.info("Phase 1: Building Input Tables...")
    sdm = pynza.Database(NZ_CONNECTION_STRING)
    
    input_member_table = sdm_batch + '_GROUPER_DXCG_MEDICARE_MEMBER_INPUT_'+ run_out + 'MO_' + str(current_iteration)
    sdm.drop(input_member_table)
    logger.info("Building member input table " + input_member_table)  
    sdm.run_script(DXCG_MEMBER_INPUT_TABLE_SCRIPT,
                    {'$SDM':sdm_batch,
                    '$YYYYMM':current_iteration,
                     '$YYYYM1':source_iteration,
                    '$RO':run_out,
                    '$DD':current_iteration_date,
                    '$START_DATE':start_date,
                    '$RUN_OUT_DATE':run_out_date,
                    '$END_DATE':end_date})

    
    input_diagnosis_table = sdm_batch + '_GROUPER_DXCG_MEDICARE_DIAGNOSIS_INPUT_'+ run_out + 'MO_'  + str(current_iteration)
    sdm.drop(input_diagnosis_table)
    logger.info("Building diagnosis input table " + input_diagnosis_table)
    sdm.run_script(DXCG_DIAGNOSIS_INPUT_TABLE_SCRIPT,
                    {'$SDM':sdm_batch,
                    '$YYYYMM':current_iteration,
                     '$YYYYM1':source_iteration,
                    '$DD':current_iteration_date,
                    '$RO':run_out,
                    '$RUN_OUT_DATE':run_out_date,
                    '$START_DATE':start_date,
                    '$END_DATE':end_date})


                     
    ##########        Phase 2: Build Input files
    
    logger = logging.getLogger('DxCG_Build_Input_Files')
    logger.info("Phase 2: Building Input Files...")
    logger.info("Building member input file " + input_member_file)
    sdm = pynza.Database(NZ_CONNECTION_STRING)
    dxcg_bound_table = sdm.get_table(input_member_table)
    dxcg_bound_table = dxcg_bound_table.as_df()
    dxcg_bound_table.sort_values(['MEMBER_ID'],inplace=True)
    input_file = open(input_member_file,"w")
    input_file.write(dxcg_bound_table.to_string(header=False,index=False,
                                                float_format=lambda x: '%.2f' % x))
    input_file.close()

    logger.info("Building diagnosis input file " + input_diagnosis_file)
    sdm = pynza.Database(NZ_CONNECTION_STRING)
    dxcg_bound_table = sdm.get_table(input_diagnosis_table)
    dxcg_bound_table = dxcg_bound_table.as_df()
    dxcg_bound_table.sort_values(['MEMBER_ID'],inplace=True)
    input_file = open(input_diagnosis_file,"w")
    input_file.write(dxcg_bound_table.to_string(header=False,index=False))
    input_file.close()



    ##########        Phase 3: Running DxCG for input files
def run_dxcg_grouper(sdm_batch, current_iteration, source_iteration, current_iteration_date,
                     executable, config_file, output_file, input_member_file, input_diagnosis_file,run_out):

    logger = logging.getLogger('DxCG_Run_Grouper')
    logger.info("Phase 3: Running DxCG for Input Files...")
    
    try:
        new_cfg = open(os.path.dirname(os.path.abspath(__file__)) + r'/lastconfig.cfg','w')
        old_cfg = open(config_file,'r')
        for line in old_cfg:
            line = line.replace('$member_file', input_member_file)
            line = line.replace('$diagnosis_file', input_diagnosis_file)
            line = line.replace('$out_file',output_file)
            line = line.replace('$path',PATH)
            new_cfg.write(line)
        old_cfg.close()
        new_cfg.close()
        new_cfg = os.path.dirname(os.path.abspath(__file__)) + r'/lastconfig.cfg'
                    
        cmd = "\"" + executable + "\""
        cmd += " \"" + new_cfg + "\""
        result = subprocess.check_output(cmd,
                                         stderr=subprocess.STDOUT,
                                         shell=True)        
        

    except subprocess.CalledProcessError as grouper_error:
        if grouper_error.returncode == 5030:
            ##########        Phase 4: Loading DxCG output file

            logger = logging.getLogger('DxCG_Build_Output_Table')
            logger.info("Phase 4: Building Output Table...")
        
            sdm = pynza.Database(NZ_CONNECTION_STRING)
            stage_table=  sdm_batch + '_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE_'+ run_out + 'MO_' + str(current_iteration)
            sdm.drop(stage_table)
            logger.info("Building dxcg stage table  " + stage_table)
            sdm.new_table(stage_table,DXCG_COLUMNS)
            dxcg_results_table = sdm.get_table(stage_table)

            dxcg_results_table.load_csv(output_file,
                                        delimiter= '#',
                                        no_headers=True)

            output_table=  sdm_batch + '_GROUPER_DXCG_MEDICARE_OUTPUT_' + run_out + 'MO_'+ str(current_iteration)
            sdm.drop(output_table)
                      
            logger.info("Building dxcg output table  " + output_table)
            sdm.run_script(DXCG_OUTPUT_TABLE_SCRIPT,
                            {'$SDM':sdm_batch,
                             '$RO':run_out,
                             '$DD':current_iteration_date,
                             '$YYYYM1':source_iteration,
                            '$YYYYMM':current_iteration})

            stage_table = sdm_batch + '_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE_' + run_out + 'MO_'+ str(current_iteration)
            sdm.drop(stage_table)
            stage_table = sdm_batch + '_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE3_' + run_out + 'MO_'+ str(current_iteration)
            sdm.drop(stage_table)
         
            
        else:
            log_string = "\n\nDxCG Build Complete. "+ str(grouper_error.cmd) + "\n"
            log_string += "DxCG Log files contained in "
            log_string += output_file.rstrip('dxcgout.txt')
            logger.info(log_string)

            log_string = "\n\nError running Grouper: "+ str(grouper_error.cmd) + "\n"
            log_string += "Grouper return code: "+ str(grouper_error.returncode) + "\n"
            log_string += "Grouper output: \n"+ str(grouper_error.output) + "\n"
            logger.error(log_string)
            raw_input("Press Enter to Quit.")
            exit()

    
       ##########        Phase 5: Append additional member data to output table
def append_additional_data(sdm_batch, current_iteration, source_iteration, current_iteration_date,
                           start_date, end_date, run_out):
    
    logger = logging.getLogger('DxCG_Append_Data')
    logger.info("Phase 5: Appending member data and finalizing data")

    sdm = pynza.Database(NZ_CONNECTION_STRING)
    sdm.drop("DXCG43_MEDICARE_" + run_out + "MO_" +current_iteration)
    sdm.run_script(DXCG_APPEND_OUTPUT_TABLE_SCRIPT,
                            {'$SDM':sdm_batch,
                             '$DD':current_iteration_date,
                            '$RO':run_out,
                             '$YYYYM1':source_iteration,
                            '$YYYYMM':current_iteration,
                            '$START_DATE':start_date,
                            '$END_DATE':end_date})
    
    output_table =  sdm_batch + '_GROUPER_DXCG_MEDICARE_OUTPUT_' + run_out + 'MO_'+ str(current_iteration)
    sdm.drop(output_table)
    
    logger.info("DxCG Process Complete for iteration " + current_iteration + "\n\n")

##def add_fields(sdm_batch, current_iteration, current_iteration_date,
##                           start_date, end_date, run_out):
##    logger = logging.getLogger('DxCG Add Fields')
##    logger.info("Phase 5: Adding fields")
##
##    sdm = pynza.Database(NZ_CONNECTION_STRING)
##    sdm.run_script('C:/Users/ADMJdsouz01/Desktop/ADD_FIELDS.sql',
##                            {'$SDM':sdm_batch,
##                             '$DD':current_iteration_date,
##                            '$RO':run_out,
##                            '$YYYYMM':current_iteration,
##                            '$START_DATE':start_date,
##                            '$END_DATE':end_date})
##    logger.info("Fields Added\n\n")

def main():
    """Confirm settings with user and run functions"""
    
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)

    # INFO logging to console
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # DEBUG logging to file
    handler = logging.FileHandler('dxcg_build.log')
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('-- %(asctime)s - %(name)s - %(levelname)s :\n %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    input_member_file = PATH + "/DxCG_Member_Input.txt"
    input_diagnosis_file = PATH + "/DxCG_Diagnosis_Input.txt"
    output_file = PATH + "/dxcgout.txt"
    executable = "D:/Apps/Program Files/DxCG/DxCG Intelligence 4/dxcg.exe"
    config_file = PATH + "/config.cfg"

    current_iteration = datetime.strftime(datetime.now() - relativedelta(months=1), "%Y%m")
    current_iteration = str(raw_input("Production Month ["+current_iteration+"]: ")) or current_iteration
    current_iteration_date = '01'
    run_out = '0'
    
    sdm_batch = raw_input("Production Batch [SDM]: ") or 'JDD'

    input_member_file = raw_input("Member File [" + input_member_file + "]: ") or input_member_file
    input_diagnosis_file = raw_input("Diagnosis File [" + input_diagnosis_file + "]: ") or input_diagnosis_file
    output_file = raw_input("DxCG Output File [" + output_file + "]: ") or output_file
    executable = raw_input("DxCG executable [" + executable + "]: ") or executable
    config_file = raw_input("DxCG Config File [" + config_file + "]: ") or config_file
    run_out = raw_input("Run Out [" + run_out + "]: ") or run_out
    
    
    if not all([os.path.exists(filename)
                for filename in [os.path.dirname(input_member_file),
                                 os.path.dirname(input_diagnosis_file),
                                 os.path.dirname(executable),
                                 os.path.dirname(config_file)]]):
        logger.error("DxCG Grouper Directories/Executable not found.")
        logger.error("Quitting.")
        raw_input("Press Enter to Quit.")
        exit()

##    for iterator in range(1, 16):
        

    start_date = current_iteration[0:4] + "-" + current_iteration[4:6] + "-" + current_iteration_date
    start_date = datetime.strptime(start_date,"%Y-%m-%d")
    
    run_out_date = start_date + relativedelta(months=int(run_out) + 1,days=-1)
    end_date = start_date + relativedelta(months=1,days=-1) 
    start_date = start_date + relativedelta(months=-11)
    
    start_date   = "\'" + datetime.strftime(start_date,"%Y-%m-%d") + "\'"
    end_date     = "\'" + datetime.strftime(end_date,"%Y-%m-%d") + "\'"
    run_out_date = "\'" + datetime.strftime(run_out_date,"%Y-%m-%d") + "\'"

    
    if re.search(r'12$',current_iteration):
        source_iteration = str(int(current_iteration[:4]) + 1) + '01'
    else:    
        source_iteration = str(int(current_iteration) + 1)
    

    logger.info("Start Date       : " + start_date)
    logger.info("End Date         : " + end_date)
    logger.info("Run Out          : " + run_out_date)
    logger.info("Source Iteration : " + source_iteration)

        
    build_dxcg_input(sdm_batch, current_iteration, source_iteration, current_iteration_date,
                input_member_file, input_diagnosis_file, start_date, end_date, run_out,
                run_out_date)
                                          
    run_dxcg_grouper(sdm_batch, current_iteration, source_iteration, current_iteration_date,
                 executable, config_file, output_file, input_member_file, input_diagnosis_file,
                 run_out)
                                          
    append_additional_data(sdm_batch, current_iteration, source_iteration, current_iteration_date,
                        start_date, end_date, run_out)



##    add_fields(sdm_batch, current_iteration, current_iteration_date,
##                        start_date, end_date, run_out)

    
##        if int(current_iteration) == 201412:
##            current_iteration = '201501'
##        elif int(current_iteration) == 201512:
##            current_iteration = '201601'
##      else:    
##            current_iteration = str(int(current_iteration) + 1)



if __name__ == "__main__":
    main()

