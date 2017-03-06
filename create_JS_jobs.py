#!/usr/bin/python
import os
import sys

# Create the kerenelgateway api calls
def create_cell_api_calls():
    number_of_jobs = raw_input("Number of cells: ")
    notebook = raw_input("Name of notebook (DONT use [] if they are in file name): ")
    ip = raw_input("Internal IP of server: ")
    port = raw_input("Port to make calls to: ")
    job_range = range(1, int(number_of_jobs) + 1)
    for n in job_range:
        file(src + 'cell%s.job.xml' % n, 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job  order="yes" stop_on_error="no" title="cell%s">
    <script  language="shell">
        <![CDATA[
curl -i -X GET http://%s:%s/%s > /tmp/%s_status
/opt/jobscheduler_jobs/scripts/check_jupyter_success.sh /tmp/%s_status
        ]]>
    </script>

    <run_time />
</job>
""" % (n, ip, port, n, notebook, notebook)
        )

def create_start_kernel_job():
    notebook = raw_input("Name of notebook (use [] if they are in file name): ")
    spark = raw_input("Spark (y/n)?: ")
    notebook_dir = raw_input("Absolute path to notebook parent dir: ")
    port = raw_input("Port to run kerenelgateway on: ")
    agent = raw_input("Name of agent: ")

    path_without_home = "/".join(notebook_dir.split("/")[3:])

    if spark == "y":
        file(src + 'start_kernel.job.xml', 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job  order="yes" stop_on_error="no" title="Start kernel" process_class="sparkm2_agent">
    <script  language="shell">
        <![CDATA[
export SPARK_HOME=/home/rifiniti/spark
export PATH=/home/rifiniti/anaconda2/bin:$SPARK_HOME/bin:$PATH
export PYTHONPATH=/home/rifiniti/spark/python/lib/py4j-0.10.3-src.zip:/home/rifiniti/spark/python:/home/rifiniti/spark/python/build
export PYTHONPATH=$PYTHONPATH:%s
/home/rifiniti/start_kernel.sh /%s %s
sleep 5
        ]]>
    </script>

    <run_time />
</job>
 """ % (notebook_dir, path_without_home, port)
        )
    else:
        file(src + 'start_kernel.job.xml', 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job  order="yes" stop_on_error="no" title="Start kernel" process_class="%s">
    <script  language="shell">
        <![CDATA[
export PYTHONPATH=/home/rifiniti/spark/python/lib/py4j-0.10.3-src.zip:/home/rifiniti/spark/python:/home/rifiniti/spark/python/build
export PYTHONPATH=$PYTHONPATH:%s
/home/rifiniti/start_kernel.sh /%s %s
sleep 5
        ]]>
    </script>

    <run_time />
</job>
# """ % (agent, notebook_dir, path_without_home, port)
        )

def create_kill_kernel_job():
    notebook = raw_input("Name of notebook (Exclude the [] and everything between them): ")
    agent = raw_input("Name of agent: ")

    file(src + 'kill_kernel.job.xml', 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job  order="yes" stop_on_error="no" title="Kill Kernel" process_class="%s">
    <script  language="shell">
        <![CDATA[
/home/rifiniti/kill_kernel.py %s
        ]]>
    </script>

    <run_time />
</job>
""" % (agent, notebook)
    )

def create_job_chain():
    notebook = raw_input("Name of notebook (DONT use [] if they are in file name): ")
    number_of_jobs = raw_input("Number of cells: ")
    job_range = range(1, int(number_of_jobs) + 1)
    state = 200

    file(src + "%s.job_chain.xml" % notebook, 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job_chain  title="%s">
    <job_chain_node  state="100" job="start_kernel" next_state="200" error_state="error"/>
""" % (notebook)
    )

    for n in job_range:
        file(src + "%s.job_chain.xml" % notebook, 'a').write(
    """
    <job_chain_node  state="%s" job="cell%s" next_state="%s" error_state="error"/>
    """ % (state, n, state + 100)
        )
        state += 100

    file(src + "%s.job_chain.xml" % notebook, 'a').write(
    """
    <job_chain_node  state="%s" job="kill_kernel" next_state="success" error_state="error"/>

    <job_chain_node  state="success"/>

    <job_chain_node  state="error"/>
</job_chain>
""" % (state)
    )

try:
    src = sys.argv[1]
    function = sys.argv[2]

    if function == "api_calls":
        create_cell_api_calls()
    elif function == "start_kernel":
        create_start_kernel_job()
    elif function == "kill_kernel":
        create_kill_kernel_job()
    elif function == "job_chain":
        create_job_chain()
except IndexError:
    print(
    """
    Use
    -----------------------
    /<path to script> <dir where job chain will be> <option>

    Options
    -----------------------
    api_calls: Create jobs for api calls to cells
    start_kernel: Create the job that starts the jupyter kernel gateway kernel
    kill_kernel: Create the job to kill the jupyter kernel gateway kernel
    job_chain: Create the job chain
    """
    )
