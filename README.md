# Monitoramento das ocorrências de responsabilidade da CET-RIO

## Prerequisites
* [Prefect 2.x](https://docs.prefect.io/) 

## Install
0. If not installed, install `prefect`:
    ```
    pip3 install prefect
    ```
    Also, we recommend the use of a virtual environment.
1. Build the prefect flow:
    ```
    prefect deployment build etl_secmuntrans_rio.py:pipeline_ocorrencias_por_pop -n etl_smtr -t production -v 0.1 --interval 1200
    ```

2. Set your favourite working directory (where the output .csv file will appear) in `pipeline_ocorrencias_por_pop-deployment.yaml`:
    ```
    ...
    infrastructure:
    ...
      working_dir: /opt/secmuntrans_rio
    ...
    ```

3. (OPTIONAL) Increment the anchor date by a few (~5) minutes so that the script goes straight to work:
    ```
    ...
    schedule:
      ...
      anchor_date: '2023-07-05T21:30:38.605147+00:00'
    ...
    ```

4. Apply the .yaml file with prefect:
    ```
    prefect deployment apply pipeline_ocorrencias_por_pop-deployment.yaml
    ```

5. Start the orion workflow engine:
    ```
    prefect orion start
    ```
    We recommend to run orion independently of the current terminal session, in this case, use:
    ```
    nohup prefect agent start -q default > nohup.out &2>1 &
    ```

6. Start agent responsible for the default queue:
    ```
    prefect agent start -q default
    ```
    If you wish to make the agent persist beyond the current terminal session, run instead:
    ```
    nohup prefect agent start -q default > nohup.out &2>1 &
    ```

7. Check the schedule of flows with the command:
    ```
    prefect flow-run ls
    ```
    It should show scheduled runs every twenty minutes from the chosen `anchor_date` onwards.

## Notes
I used this challenge as an exercise to get to know the data orchestration tool `prefect`. It has been a worthy exercise, in particular due to the functionality of allowing a variable number of retries for certain tasks and due to straightforward error logging. Usually, I would implement a project like this with asyncio and logging, however, I clearly see the advantages of prefect here.

Yet, the documentation of the different of versions of prefect that are around seems to be sparse and rather confusing in comparison with, for instance, asyncio. It surprises me that there seems to be no straightforward way of canceling flows whose schedule was missed. My curiosity is sparked to find out more. 

Normally I would either implement a schedule directly through crontab or use a systemd timer, both of which do not spawn missed scheduled runs after the moment has passed, let us say due to a reboot or power outage.
