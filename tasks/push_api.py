from cumulusci.tasks.salesforce import BaseSalesforceApiTask

class SyncPushErrors(BaseSalesforceApiTask):
    task_options = {
        "offset": {
            "description": "Offset to use in SOQL query of PackagePushError",
       }
    }
    def _init_options(self, kwargs):
        # creating package id initialization not sure its 
        # necessary at this point
        self.package_id = None
        super(SyncPushErrors, self)._init_options(kwargs)
        # Set the namespace option to the value from cumulusci.yml if not already set
        # if "namespace" not in self.options:
        self.options["namespace"] = self.project_config.project__package__namespace

    def _run_task(self):
        # Query PackagePushErrors
        # not correct query but leaving for now
        # self.api.query("SELECT * FROM packagepusherror;")
        # Get heroku postgres service
        # service = self.project_config.keychain.get_service("metapush_postgres")
        # Initialize a postgres connection
        # Upsert results to postgres
        print("hello")
