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
        cur.execute("SELECT MAX(push_error_time) from pushupgrades")
        last_run = cur.fetchone()[0]
        print(last_run)

        self.job_query = "SELECT Id, PackagePushJobId, ErrorMessage, ErrorDetails, ErrorTitle, ErrorSeverity, ErrorType, SystemModstamp FROM PackagePushError LIMIT 2"  # LIMIT  50000 OFFSET 10 "

        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no more SQL injections!)
        # cur.execute("SELECT * FROM pushupgrades;")

        # Query the database and obtain data as Python objects
        # cur.execute("SELECT MAX() FROM test;")

        # Make the changes to the database persistent
        conn.commit()

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

        # Sort by error title
        for records in job_records[:]:
            # print(records)
            row = {}
            for _, (k, v) in enumerate(records.items()):
                # skipping unwanted attribute key values storing all others
                # to be upserted
                if k.lower() != "attributes":
                    row[k] = v
            print(row)
            print()
        # Close communication with the database
        cur.close()
        conn.close()


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
