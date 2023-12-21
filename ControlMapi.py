import requests
import json

def get_controlm_job_state(server_url, username, password, job_name):
    # Set the Control-M API endpoint
    api_endpoint = f"{server_url}/automation-api"

    # Authenticate and obtain an access token
    auth_url = f"{api_endpoint}/authentication/login"
    auth_data = {"username": username, "password": password}
    auth_response = requests.post(auth_url, data=json.dumps(auth_data), headers={"Content-Type": "application/json"})
    auth_response.raise_for_status()
    access_token = auth_response.json()["token"]

    # Get the job details
    job_url = f"{api_endpoint}/run/jobs"
    job_params = {"filter": f'{{"name": "{job_name}"}}'}
    job_headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    job_response = requests.get(job_url, params=job_params, headers=job_headers)
    job_response.raise_for_status()

    # Parse the response and extract the job state
    job_data = job_response.json()
    if job_data["total"] == 0:
        return f"Job '{job_name}' not found"
    else:
        job_state = job_data["data"][0]["status"]
        return f"The state of job '{job_name}' is {job_state}"

if __name__ == "__main__":
    # Replace these values with your Control-M server details
    controlm_server_url = "http://your-controlm-server:port"
    controlm_username = "your_username"
    controlm_password = "your_password"
    job_to_check = "YourJobName"

    job_state = get_controlm_job_state(controlm_server_url, controlm_username, controlm_password, job_to_check)
    print(job_state)
