import requests
import json


def make_POST(
        url,
        selected_authors,
        selected_pubs,
        selected_level,
        selected_scheme,
        selected_cutoff,
        selected_cutoff_terms,
        selected_include_common_terms,
        selected_a_path):
    data = {
        'selected_authors': selected_authors,
        'selected_works_id': selected_pubs,
        'level': selected_level,
        'selected_scheme_id': selected_scheme,
        'cutoff_value': selected_cutoff,
        'cutoff_terms_value': selected_cutoff_terms,
        'include_common_terms': selected_include_common_terms,
        'include_management_theory': 'значение_include_management_theory',
        'path': selected_a_path
    }

    headers = {'Content-Type': 'application/json'}
    json_data = json.dumps(data)

    response = requests.post(url, data=json_data, headers=headers)
    return response


labs_request = requests.get('http://193.232.208.28/api/v2.0/organization?id=1')
labs_list = []
labs_dict = {}

if labs_request.status_code == 200:
    try:
        labs_data = labs_request.json()
        arr = []
        for i in labs_data:
            if i['div_id'] == "95":
                continue
            labs_list.append({"label": i['div_name'], "value": i['div_id']})
            labs_dict[int(i['div_id'])] = i['div_name']
    except json.JSONDecodeError:
        print("Error decoding JSON response PUBS")
else:
    print(f"Request failed with status code: {labs_request.status_code}")

url = "http://193.232.208.58:5001/post_labs"
excepted_labs = []
print(labs_dict)
for i in labs_dict:
    if str(i) in ['16', '61', '84', '108', '85', '109', '87', '96', '67', '32', '83', '18', '58', '79', '127', '166']:
        continue
    response = make_POST(
        url,
        [int(i)],
        [],
        1,
        4,
        1,
        1,
        True,
        [],
    )
    if response.status_code != 200:
        print("exception")
        excepted_labs.append(i)
print(excepted_labs)
