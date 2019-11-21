import os
import psycopg2
from cumulusci.tasks.salesforce import BaseSalesforceApiTask

# from cumulusci.tasks.salesforce import BaseSalesforceToolingApiTask


class SyncPushErrors(BaseSalesforceApiTask):
    task_options = {
        "offset": {"description": "Offset to use in SOQL query of PackagePushError"}
    }

    def _init_options(self, kwargs):
        super(SyncPushErrors, self)._init_options(kwargs)
        # Set the namespace option to the value from cumulusci.yml if not already set
        # if "namespace" not in self.options:
        self.options["namespace"] = self.project_config.project__package__namespace

    def _run_task(self):
        # Get heroku postgres service
        service = self.project_config.keychain.get_service("metapush_postgres")
        DATABASE_URL = service.db_url
        # Initialize a postgres connection
        # try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        # Query PackagePushErrors

        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command that gets the last time this report was run
        cur.execute("SELECT MAX(last_run) from pushupgrades;")
        last_run = cur.fetchone()[0]
        # print(last_run.replace(" ", "T"))
        self.job_query = f"SELECT Id, PackagePushJobId, ErrorMessage, ErrorDetails, ErrorTitle, ErrorSeverity, ErrorType, SystemModstamp FROM packagePushError WHERE sysmodstamp > {last_run}"
        # 2019-11-06T05:06:33.000+0000"
        # print(self.job_query)

        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no more SQL injections!)
        # cur.execute("SELECT * FROM pushupgrades;")

        formatted_query = self.job_query.format(**self.options)
        self.logger.debug("Running query for job errors: " + formatted_query)
        result = self.sf.query(formatted_query)
        job_records = result["records"]
        self.logger.debug(
            "Query is complete: {done}. Found {n} results.".format(
                done=result["done"], n=result["totalSize"]
            )
        )
        if not result["totalSize"]:
            self.logger.info("No errors found.")
            return

        offset = cur.execute("SELECT COUNT(*) FROM pushupgrades;")
        offset = cur.fetchone()[0]
        print("OFFSET: ", offset)
        id = 0 + offset + 1
        for records in job_records:
            row = {}
            for _, (k, v) in enumerate(records.items()):
                # skipping unwanted attribute key values storing all others to be upserted
                if k.lower() != "attributes":
                    row[k] = v
            self.logger.info(row)
            self.logger.info("")
            cur.execute(
                "INSERT INTO gem.packagepusherror (systemmodstamp,errortype,errortitle,errorseverity,errormessage,errordetails,packagepushjobid,sfid,id,_hc_lastop,_hc_err) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s) ON CONFLICT (id) DO NOTHING;",  # ON CONFLICT DO NOTHING",
                (
                    row["SystemModstamp"],
                    row["ErrorType"],
                    row["ErrorTitle"],
                    row["ErrorSeverity"],
                    row["ErrorMessage"],
                    row["ErrorDetails"],
                    row["PackagePushJobId"],
                    row["Id"],
                    id,
                    "NULL",
                    "NULL",
                ),
            )
            id += 1
            conn.commit()
        # Close communication with the database
        cur.close()
        conn.close()
