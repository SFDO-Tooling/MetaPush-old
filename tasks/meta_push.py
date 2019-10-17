from cumulusci.tasks.salesforce import BaseSalesforceApiTask
# from cumulusci.tasks.salesforce import BaseSalesforceToolingApiTask

class SyncPushErrors(BaseSalesforceApiTask):
    task_options = {
        "offset": {
            "description": "Offset to use in SOQL query of PackagePushError",
       }
    }

    def _init_options(self, kwargs):
        super(SyncPushErrors, self)._init_options(kwargs)
        # Set the namespace option to the value from cumulusci.yml if not already set
        # if "namespace" not in self.options:
        self.options["namespace"] = self.project_config.project__package__namespace
        self.job_query = "SELECT (SELECT ErrorDetails, ErrorMessage, ErrorSeverity, ErrorTitle, \
                          ErrorType FROM PackagePushErrors) FROM PackagePushJob " #LIMIT  50000 OFFSET 10 "

    def _run_task(self):
        # Query PackagePushErrors
        count = 0
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
            count += 1
            for message, record in records.items():
                if record is None:
                    # count += 1
                    continue
                # omitting attributes key or any others with non error pertaining information
                if message.lower() == 'packagepusherrors':
                    # count += 1
                    for _ ,(k,v) in enumerate(record.items()):
                        # omitting non error related tuples
                        if k.lower() == 'records':
                            row = {}
                            for _ ,(error_key,error_value) in enumerate(v[0].items()):
                                if not error_key.lower() == 'attributes':
                                    row[error_key] = error_value
                                else: 
                                    continue
                            self.logger.info(row)
                        else:
                            continue
                else:
                    continue

        # Get heroku postgres service

        # service = self.project_config.keychain.get_service("metapush_postgres")

        # Initialize a postgres connection

        # Upsert results to postgres

    # what the query should be if it would accept 
    def _run_SOQL_query(self, query):
        res = self.sf.query(query)
        for contact in res['records']:
            self.logger.info('{AccountId}'.format(**contact))