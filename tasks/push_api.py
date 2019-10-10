from cumulusci.tasks.salesforce.BaseSalesforceApiTask

class SyncPushErrors(BaseSalesforceApiTask):
    task_options = {
        "offset": {
            "description": "Offset to use in SOQL query of PackagePushError",
        }
    }

    def _run_task():
        # Query PackagePushErrors
        self.api.query(SOQL_HERE)

        # Get heroku postgres service
        service = self.project_config.keychain.get_service("metapush_postgres")

        # Initialize a postgres connection
        
        # Upsert results to postgres
