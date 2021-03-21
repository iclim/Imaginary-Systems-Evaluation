import pandas as pd #to read JSON data.

import dateutil.parser #Log files are already sorted by time, but this may prove to be useful for future application.


#User-defined microservice names based on the name of log files. 
all_microservices = ['alpha', 'beta', 'gamma', 'delta', 'epsilon']

#user-defined microservice connections. 
alpha_inputs = []
alpha_outputs = ['beta', 'gamma']

beta_inputs = ['alpha']
beta_outputs = ['delta', 'epsilon']

gamma_inputs = ['alpha']
gamma_outputs = ['delta', 'epsilon']

delta_inputs = ['beta', 'gamma']
delta_outputs = ['epsilon']

epsilon_inputs = ['beta', 'gamma', 'delta']
epsilon_outputs = []



def extract_logs():
    #function for loading logs into pandas dataframes
    #returns a dictionary where keys are 'greek_letter' and items are dataframes. 
    
    d = {}
    
    for greek_letter in all_microservices:
        d[greek_letter] = pd.read_json('data/'+greek_letter+'.log',lines = True) #Edit path based on where log files are located. 
        
        d[greek_letter] = d[greek_letter].rename(columns={'text':'target','ts':'time'}) 
        
        d[greek_letter]['target'] = d[greek_letter]['target'].map(lambda x: (x.split())[-1])

    return d;



def log_summary(d, micro_service):
    #input dictionary and 'greek_letter'
    #outputs summary grouped by success of call.
    
    data = d[micro_service]['target'].groupby(d[micro_service]['code']).value_counts()
    
    return print('All calls and destinations \n', data)



def actual_vs_expected_traffic(d, micro_service):
    #input dictionary and 'greek_letter'
    #outputs amount of calls inside a log compared to calls actually sent to this log
    
    inputs = eval(micro_service+'_inputs') #access the array that gives upstream connected micro-services
    
    actual_traffic = len(d[micro_service]['target']) 
    
    if len(inputs) == 0:
        return (actual_traffic,actual_traffic) #if a micro-service has no inputs (e.g. alpha), assume traffic normal.
    
    expected_traffic = 0
    
    for x in inputs:
        expected_traffic = expected_traffic + d[x]['target'].value_counts()[micro_service]
    
    return actual_traffic, expected_traffic



def find_all_traffic_problems(d):
    #input dictionary of all dataframes
    #output summary of all issues occuring.
    
    for greek_letter in all_microservices:
        actual,expected = actual_vs_expected_traffic(d, greek_letter)
    
        if actual == expected:
            pass
    
        if actual < expected:
            print(greek_letter + ' is missing calls.','receiving:', actual, 'expecting:', expected, "\n")
    
        if actual > expected:
            print(greek_letter + ' is receiving unexpected calls.', 'receiving:', actual, 'expecting:', expected, "\n")
            


def find_unexpected_connectivity(d, micro_service):
    #input dictionary of data and 'greek_letter'
    #outputs if a micro-service is contains unexpected calls 
    
    other_services = all_microservices.copy() #consider all micro-services
    
    other_services.remove(micro_service) #remove the one in question
    
    for service in eval(micro_service+'_inputs'): #remove expected micro-services
        other_services.remove(service)
    
    flag = 0
    
    for greek_letter in other_services: 
        #check if there exists calls from other micro-services
        
        try: 
            
            num_unexpected_logs_sent = d[greek_letter]['target'].value_counts()[micro_service]
            print(num_unexpected_logs_sent, 'unexpected calls from', greek_letter, 'in the log of', micro_service,'\n')
            
            flag = 1
            
        except:
            pass
    
    if flag == 0:
        return 'No unexpected calls in ' + micro_service
    


def find_missing_calls(d, micro_service):
    #input the dictionary of data frames and 'greek_letter' 
    #output where calls went missing
    
    if len(eval(micro_service + '_inputs')) == 0: #exclude sources outside of scope 
        print('Previous logs out of scope.')
        
    for inputs in eval(micro_service + '_inputs'):
        #go into previous logs and find the code 500 entries that are missing
        
        try: 
            num_missing = d[inputs]['target'].groupby(d[inputs]['code']).value_counts()[500][micro_service]
            print(num_missing,'missing calls from', inputs, '\n')
    
        except:
            print('There are no missing calls from', inputs, '\n')   
     
    

def find_first_error(d, micro_service):
    #input the dictionary of data frames and 'greek_letter'
    #output time of first code 500 error
    
    error_time_occurance = d[micro_service]['time'].where(d[micro_service]['code'] == 500)
    
    error_time_occurance = error_time_occurance.dropna()
    
    error_time_occurance = error_time_occurance.map(lambda x: dateutil.parser.parse(x)) #convert string in time column to datetime data
    
    error_time_occurance = error_time_occurance.sort_values().reset_index()
    
    return error_time_occurance['time'][0]



def list_communication_by_time(d, micro_service_source, micro_service_target, code):
    #input the dictionary of data frames, two 'greek_letters' and either 'all', 200, 500
    #output times when calls were passed
    
    if code == 'all':
        occurances_by_time = d[micro_service_source]['time'].where(d[micro_service_source]['target'] == micro_service_target)
        occurances_by_time = occurances_by_time.dropna()
    
    else:
        occurances_by_time = d[micro_service_source]['time'].where(
        (d[micro_service_source]['target'] == micro_service_target) & (d[micro_service_source]['code'] == code))
        
        occurances_by_time = occurances_by_time.dropna()
    
    occurances_by_time = occurances_by_time.map(lambda x: dateutil.parser.parse(x)) #convert string in time column to datetime data
    
    occurances_by_time = occurances_by_time.sort_values().reset_index()
    
    return occurances_by_time['time']