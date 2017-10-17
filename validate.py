from urllib.request import urlopen
from contextlib import closing
from numbers import Number
import multiprocessing as mp
import json

def pull_from_api( site, page=0):
	'''
	pull_from_api
	retrieves JSON object with Validations, Customers, and Pagination 
	indicated by site
	@param site : url to connect to
	@param page : int with the page to query, defaulted to 0
	gets json data from api endpoint, appends page query if value of page > 0
	'''
	if(page > 0):
		site = site + "?page=" + str(page)
	with closing(urlopen(site)) as api_page:
		data = json.loads(api_page.read().decode())

	if data is not None:
		return data
	return {}

def async_api_pull(site,q=None,page=0):
	'''
	async_api_pull
	the same as pull_from_api except that it writes to a multiprocessing queue
	rather than 
	'''
	if(page > 0):
		site = site + "?page=" + str(page)
	with closing(urlopen(site)) as api_page:
		data = json.loads(api_page.read().decode())

	if data is not None:
		q.put(data.get("customers"))
		return
	q.put({})

def check_types(type_reqs={}, field=None):
	'''
	check_types
	checks the type field of the given customer against given requirements
	@param type_reqs : JSON Dict with requirements for types
	@param field     : field to be evaluated
	'''
	if type_reqs == "string":
		return isinstance(field, str)
	elif type_reqs == "boolean":
		return isinstance(field, bool)
	elif type_reqs == "number":
		return isinstance(field, Number)
	else:
		return False
def check_field(field, value, reqs={}):
	'''
	check_field
	@param field : the key of the field to be checked
	@param value : the value of the field to be checked
	@param reqs
	driver function for validating customer fields
	returns True if customer is vali, False otherwise 
	'''
	if value is None:
		if reqs["required"]:
			return False
		# return True

	else:
		user_valid = True
		if "type" in reqs:
			user_valid = check_types(type_reqs=reqs["type"], field=value)

		if not user_valid:
			return False
		
		if "length" in reqs:
			value_len = len(value)
			req_len   = reqs["length"]
			min_req   = "min" in reqs["length"]
			max_req   = "max" in reqs["length"]

			if min_req and max_req:
				min_req   = reqs["length"]["min"]
				max_req   = reqs["length"]["max"]
				user_valid     = min_req <= value_len and value_len <= max_req	
			else:
				if min_req:
					min_req   = reqs["length"]["min"]
					user_valid = min_req <= value_len
				else:
					user_valid = reqs["length"]["max"] >= value_len					
		
		return user_valid


def validate_customer_fields(reqs={}, customer={}):
	'''
	validate_customer_fields
	@param reqs 	: 
	@param customer : 
	returns None if no customers have invalid fields otherwise
	returns dictionary containing customer id and invalid fields
	'''
	req_dict = {}
	[req_dict.update(req) for req in reqs]

	invalid_fields = []	
	valid_cust 	   = True	
	for k, v in customer.items():
		if k in req_dict:
			if not check_field(field=k, value=v, reqs=req_dict[k]):
				invalid_fields.append(k)
				valid_cust = False

	if not valid_cust:
		return {"id": customer["id"], "invalid_fields": invalid_fields}


def validate(site):
	'''
	validate:
	@param site	 : JSON endpoint 
	driver function for validation of customers located at endpoint
	site, returns a json object that has all the invalid customers in it
	returns JSON dictionary of invalid customers
	'''
	init_json   = pull_from_api(site, page=0)

	if init_json == {}:
		return init_json

	# multiprocessing work to grab all customers across paginated JSON
	total_cust  = init_json["pagination"]["total"]
	validations = init_json.get("validations")
	page_len    = (total_cust//5) + 1
	q = mp.Queue()
	t_arr = []

	for i in range(page_len):
		t_arr.append(mp.Process(target=async_api_pull, args=(site, q, i+1,)))
		t_arr[i].start()
		
	for i in t_arr:
		i.join()

	cust_array = []
	while( not q.empty()):
		cust_array.append(q.get())

	# Validation work driver
	invalid_info = []
	for customer_page in cust_array:
		[invalid_info.append(validate_customer_fields(validations, c)) for c in customer_page]
	return { "invalid_customers": [x for x in invalid_info if x is not None] }


if __name__ == "__main__":
	print(validate("https://backend-challenge-winter-2017.herokuapp.com/customers.json"))
