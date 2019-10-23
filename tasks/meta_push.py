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
        self.job_query = "SELECT Id, PackagePushJobId, ErrorMessage, ErrorDetails, ErrorTitle, ErrorSeverity, ErrorType, SystemModstamp FROM PackagePushError LIMIT 2"  # LIMIT  50000 OFFSET 10 "

    def _run_task(self):
        # Query PackagePushErrors
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

        # Get heroku postgres service
        service = self.project_config.keychain.get_service("metapush_postgres")
        DATABASE_URL = service.db_url
        # Initialize a postgres connection
        # try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        # Sort by error title
        for records in job_records[:]:
            # print(records)
            row = {}
            for _, (k, v) in enumerate(records.items()):
                # skipping unwanted attribute key values storing all others to be upserted
                if k.lower() == "attributes":
                    continue
                else:
                    row[k] = v

        # except IOError:
        #     print("An error occured trying to read the file.")

        # except ValueError:
        #     print("Non-numeric data found in the file.")

        # except ImportError:
        #     print("NO module found")

        # except EOFError:
        #     print("Why did you do an EOF on me?")

        # except KeyboardInterrupt:
        #     print("You cancelled the operation.")

        # except:
        #     print("An error occured.")
        #     # Initialize a postgres connection

        #     # Upsert results to postgres
