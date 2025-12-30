# DE Test

## Scenario

A junior data engineer needs help with a data pipeline that they wrote for processing
weather forecasts.

"When I wrote this pipeline on 1st July my tests were passing fine. Now I've come
to do some work on it, my tests are failing. I don't understand what I've done wrong".

Your exercise is to help this fictional junior engineer troubleshoot their code
and address the issues that are causing their failing test.

### Exercise timings
* Please spend 5-7 minutes identifying issues with the code + test file and
  suggest to us a plan to make test pass and less flakey.
* 15 minutes implementing these improvements, and any other further improvements
* 5 minutes discussing further improvements we would make with more time


## Pipeline info

What the weather pipline does:

* Queries an open-meteo.com api to get some historical weather forecast data
* Transforms the data:
    - converts the json response into a dataframe with a timestamp column
    - creates a new calculated column: `approx_heat_index`,
      which is a (bad) approximation for what temperature the conditions feel like
* Stores the dataframe to a file in the `./destination/` folder.

We've provided you an example output from the api:
[api_sample.json](./api_sample.json).


## Environment setup

For this exercise you will need to setup a python virtual environment and install
the dependencies provided in [requirements.txt](./requirements.txt).

Feel free to do this in any way you are comfortable: either using the IDE of your
choice (e.g. vscode / pycharm / anaconda), or you can run the following terminal
commands relevant to your operating system:

### Bash (MacOS / Linux)

```bash
pip install --user pipx
python -m pipx ensurepath
pipx install poetry
poetry install
```

### PowerShell

```powershell
pip install --user pipx
python -m pipx ensurepath
pipx install poetry
poetry install
```


## Running the tests

Once your environment is setup, please run the tests using pytest by running
the following command from your terminal:

```
poetry run pytest
```

(or use the IDE of your choice to run the tests if you are more comfortable
with this approach).

There is only one test and, if your environment is setup correctly, it should
run but the assertion will fail.

## Task 2 - PySpark:
Examine the pyspark_coding.py. Your task is to add a new column called comfort_level that categorizes each hourly reading based on the following business rules:

"Cold": when temperature_2m is below 4.5°C
"Moderate": when temperature_2m is between 4.5°C and 5.5°C (inclusive)
"Warm": when temperature_2m is above 5.5°C

Also add a column called heat_index_diff that calculates the difference between approx_heat_index and temperature_2m."
