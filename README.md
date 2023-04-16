# RUKITA TEST - SENIOR DATA ENGINEER

This test consist of 3 cases : 
- case 1 - daily occupancy
- case 2 - conversion leads
- case 3 - scraping data - mamikost (Optional)

### Note : Image just prove insert data to datawarehouse is already success (amount maybe can change / because tweaking or improvement) before 17 Apr 2023 00:01

## How to run this application
1. Please setup `virtual-environments`
    ```
        python3 -m venv env
    ```
2. To activation the `virtual-enviroments`
    ```
        source env/bin/activate
    ```
3. After `virtual-enviroments` has been activate, than install the library
    ```
        pip install -r requirements.txt
    ```
4. Copy `.env.example` to `.env` or rename become `.env`
    ```
        GBQ_PROJECT = ""
        GBQ_DATASET = ""
        GBQ_LOCATION = ""
    ```
5. Don't forget about `key.json`, to generate `key.json` we need go to `https://console.cloud.google.com/` >> `IAM & Admin >> Service Accounts` >> `Keys` >> `Add Key` >> Pop Up Apper and choose `JSON`
6. To run case, please un-comments code 
    before
    ```
        # case_1()
        # case_2()
    ```
    after
    ```
        case_1()
        #case_2()
    ```