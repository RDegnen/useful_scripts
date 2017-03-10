#!/usr/local/bin/python
import os
import sys
import fire

# Create the kerenelgateway api calls
class Create_Chain_Parts(object):

    def __init__(self):
        self.client = 'uber'
        self.src = '/Users/ross/job_chains/uber/4_TEST/'

    def create_cell_api_calls(self, number_of_jobs, notebook, ip, port):
        # DONT use [] if they are in file name
        notebook = notebook.split('_[')[0]
        job_range = range(1, int(number_of_jobs) + 1)
        for n in job_range:
            file(self.src + 'cell%s.job.xml' % n, 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job  order="yes" stop_on_error="no" title="cell%s">
    <script  language="shell">
        <![CDATA[
curl -i -X GET http://%s:%s/%s > /tmp/%s_status
/opt/jobscheduler_jobs/scripts/check_jupyter_success.sh /tmp/%s_status /var/log/sparkm2/%s/%s.log
        ]]>
    </script>

    <run_time />
</job>
""" % (n, ip, port, n, notebook, notebook, self.client, notebook)
            )

    def create_start_kernel_job(self, notebook, spark, notebook_dir, port, agent):
        # Use [] if they are in file name
        path_without_home = "/".join(notebook_dir.split("/")[3:])

        if spark:
            file(self.src + 'start_kernel.job.xml', 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job  order="yes" stop_on_error="no" title="Start kernel" process_class="sparkm2_agent">
    <script  language="shell">
        <![CDATA[
export SPARK_HOME=/home/rifiniti/spark
export PATH=/home/rifiniti/anaconda2/bin:$SPARK_HOME/bin:$PATH
export PYTHONPATH=/home/rifiniti/spark/python/lib/py4j-0.10.3-src.zip:/home/rifiniti/spark/python:/home/rifiniti/spark/python/build
export PYTHONPATH=$PYTHONPATH:%s
/home/rifiniti/start_kernel.sh /%s/%s %s
sleep 5
        ]]>
    </script>

    <run_time />
</job>
""" % (notebook_dir, path_without_home, notebook, port)
            )
        else:
            file(self.src + 'start_kernel.job.xml', 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job  order="yes" stop_on_error="no" title="Start kernel" process_class="%s">
    <script  language="shell">
        <![CDATA[
export PYTHONPATH=/home/rifiniti/spark/python/lib/py4j-0.10.3-src.zip:/home/rifiniti/spark/python:/home/rifiniti/spark/python/build
export PYTHONPATH=$PYTHONPATH:%s
/home/rifiniti/start_kernel.sh /%s%s %s
sleep 5
        ]]>
    </script>

    <run_time />
</job>
""" % (agent, notebook_dir, path_without_home, notebook, port)
            )

    def create_kill_kernel_job(self, notebook, agent):
        # Exclude the [] and everything between them
        notebook = notebook.split('_[')[0]
        file(self.src + 'kill_kernel.job.xml', 'w').write(
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

    def create_job_chain(self, notebook, number_of_jobs):
        # DONT use [] if they are in file name
        notebook = notebook.split('_[')[0]
        job_range = range(1, int(number_of_jobs) + 1)
        state = 200

        file(self.src + "%s.job_chain.xml" % notebook, 'w').write(
"""<?xml version="1.0" encoding="ISO-8859-1"?>


<job_chain  title="%s">
    <job_chain_node  state="100" job="start_kernel" next_state="200" error_state="error"/>
""" % (notebook)
        )

        for n in job_range:
            file(self.src + "%s.job_chain.xml" % notebook, 'a').write(
    """
    <job_chain_node  state="%s" job="cell%s" next_state="%s" error_state="error"/>
    """ % (state, n, state + 100)
        )
            state += 100

        file(self.src + "%s.job_chain.xml" % notebook, 'a').write(
    """
    <job_chain_node  state="%s" job="kill_kernel" next_state="success" error_state="error"/>

    <job_chain_node  state="success"/>

    <job_chain_node  state="error"/>
</job_chain>
""" % (state)
        )

def main():
    fire.Fire(Create_Chain_Parts)

if __name__ == '__main__':
    main()
