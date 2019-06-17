###################################################################################################################
#---------------------------------- ERROR HANDLING FRAMEWORK ----------------------------------------------###
################################################################################################################
### Error handling framework is designed to automate the process in such a way that whenever an exception    ###
### occur, it will search for the exception class in JSON(which is maintained by the user) and if it matches ###
### ,then specific message defined in JSON will be printed out, along with system message. Otherwise, only   ###
### system message will be printed out.                                                                      ###
### This framework can also handle user defined exception, which can be defined under base class ErrorCode.  ###
###                                                                                                          ###
### The class Errors() takes input as the result of an exception i.e. e (Exception as e).                    ###
###                                                                                                          ###
###                                                                                                          ###
################################################################################################################
import sys
import traceback
import json


# --------------------------------------------------------------------------------------------------------------#
# ErrorCode() - Base class for exceptions in this module.
# --------------------------------------------------------------------------------------------------------------#
class ErrorCode(Exception):
    pass


# --------------------------------------------------------------------------------------------------------------#
# Whenever a new user defined exception is need to be created, cusom class should be defined or added in below
# part, with ErrorCode() as parent class for it.
# Also, new exception class along with detailed description should be added in maintained JSON file - json.txt
# --------------------------------------------------------------------------------------------------------------#
class NoRolesException(ErrorCode):
    pass


# --------------------------------------------------------------------------------------------------------------#
# Errorfunc() - Class contains below functions:
# 1. Opens the JSON file which maintains the error message and exceptions.
# 2. Error function to process string type message.
# 3. Error function to process messages of custom type.
# 4. Error function to process json type messages.
# 5. Functions that define at which particular place the error occurs.
# --------------------------------------------------------------------------------------------------------------#
class Errorfunc(ErrorCode):
    # with open('/home/dataiku/dss/uploads/AMERITAS/datasets/json2/json2.txt') as f:
    with open('./json.txt') as f:
        error_json = json.load(f)

    def __init__(self):
        print(self.msg)

    # -----------------------------------------------------------------------------------------------------------------#
    # Function for processing and printing string type messages.
    # -----------------------------------------------------------------------------------------------------------------#
    def str_err_func(self, error_id):
        user_error_message = self.error_json[error_id]['UserDesc']
        message = "Error Type     : {0} \nUser message   : {1} \nSystem message : {2}" \
            .format(self.excep_type, user_error_message, self.msg)
        print(message)

    # -----------------------------------------------------------------------------------------------------------------#
    # Function for processing and printing custom type messages.
    # -----------------------------------------------------------------------------------------------------------------#
    def custom_err_func(self, error_id):
        user_error_message = self.error_json[error_id]['UserDesc']
        message = "Error Type     : {0} \nUser message   : {1}" \
            .format(self.excep_type, user_error_message)
        print(message)

    # -----------------------------------------------------------------------------------------------------------------#
    # Function for processing and printing json type messages.
    # -----------------------------------------------------------------------------------------------------------------#
    def json_err_func(self, error_id):
        error_code = self.msg.response['Error']['Code']
        sys_error_message = self.msg.response['Error']['Message']
        resource_name = self.msg.response['Error']['Resource']
        user_error_message = self.error_json[error_id]['UserDesc']
        message = "Error Type     :   {0} \nUser message   : {1} \nSystem message : {2} \nResource name  :{3}" \
            .format(error_code, user_error_message, sys_error_message, resource_name)
        print(message)

    ###---------------------------------------------------------------------------------------------------------------###
    ### Traceback is used to get the information related to the line no., name of the file, line definition and module
    ### name where the error occured.
    ###---------------------------------------------------------------------------------------------------------------###
    def err_tb(self):
        template = "An exception of type {0} occured."
        print(template.format(type(self.msg).__name__))
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
            for n in range(0, 4):
                stack_trace.append(trace[n])
        print("In file {0}, on line {1} at statement {2} below error occured" \
              .format(stack_trace[0], stack_trace[1], stack_trace[3]))


#####################################################################################################################
### The result of an exception is either in a JSON format(which is comes from an API call) or in a string format. ###
### Below function checks the format of the error and also checks whether that particular error is documented in  ###
### error JSON(defined by user). If the error is present, then it takes the message from the error Json and prints###
### it along with the system message. If not, then it just prints the error message out from the system.          ###
#####################################################################################################################
class Errors(Errorfunc):
    def __init__(self, msg):
        self.msg = msg
        self.excep_type = msg.__class__.__name__
        self.module_name = msg.__class__.__module__
        super(Errorfunc, self).__init__()

    def errorrun(self):
        Errorfunc.err_tb(self)
        ###---------------------------------------------------------------------------------------------------------------###
        ### Checks need to be performed to determine whether an exception is user defined or built in. Once confirmed,
        ### then check for the type of error message JSON or string.
        ### Accordingly need to call the below functions:
        ### json_err_func : for json
        ### str_err_func : for string
        ### custom_err_func : for user defined exception
        ###---------------------------------------------------------------------------------------------------------------###
        error_json = self.error_json
        ### Check for custom type : User defined or built-in
        try:
            exec(self.excep_type + '()')
            excep_type = self.excep_type
            for err_id in error_json.keys():
                if excep_type in error_json[err_id]['ErrorType']:
                    exec('Errorfunc.custom_err_func(self,\'' + err_id + '\')')
                    break
                elif err_id == list(error_json.keys())[-1]:
                    print('Custom message is not available')
        except NameError as e:
            ### Check for exception result type : json or string or dictionary
            if type(self.msg) == dict:
                for err_id in error_json.keys():
                    if str(self.msg.response['Error']['Code']) in error_json[err_id]['ErrorType']:
                        exec('Errorfunc.json_err_func(self,\'' + err_id + '\')')
                        break
                    elif err_id == list(error_json.keys())[-1]:
                        print('Error type   :{}'.format(self.msg.response['Error']['Code']))
                        print('Error message:{}'.format(self.msg.response['Error']['Message']))
            else:
                excep_type = self.excep_type
                for err_id in error_json.keys():
                    if excep_type in error_json[err_id]['ErrorType']:
                        exec('Errorfunc.str_err_func(self,\'' + err_id + '\')')
                        break
                    elif err_id == list(error_json.keys())[-1]:
                        print('Error type   :{}'.format(excep_type))
                        print('Error message:{}'.format(self.msg))