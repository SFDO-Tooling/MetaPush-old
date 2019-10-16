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
        self.job_query = "SELECT ID, SubscriberOrganizationKey, (SELECT ErrorDetails, ErrorMessage, ErrorSeverity, ErrorTitle, ErrorType FROM PackagePushErrors) FROM PackagePushJob"

    def _run_task(self):
        # Query PackagePushErrors
        ### proper query but wont work need to ask 
        ### 'Select ErrorMessage, ErrorDetails, ErrorTitle, ErrorSeverity, ErrorType from PackagePushError'
        print(self._run_SOQL_query('Select AccountId from USER LIMIT 10'))

        # Get errors
        formatted_query = self.job_query.format(**self.options)
        self.logger.debug("Running query for job errors: " + formatted_query)
        result = self.sf.query(formatted_query)
        job_records = result["records"]
        # Get heroku postgres service

        # service = self.project_config.keychain.get_service("metapush_postgres")

        # Initialize a postgres connection

        # Upsert results to postgres
        print("hello world!")

    # what the query should be if it would accept 
    def _run_SOQL_query(self, query):
        res = self.sf.query(query)
        for contact in res['records']:
            self.logger.info('{AccountId}'.format(**contact))


    # def _run_SOQL_query(self, query):
    # res = self.sf.query(query)
    # for contact in res['records']:
    #     self.logger.info('ErrorDetails: {ErrorDetails} \
    #                       ErrorMessage: {ErrorMessage}  \
    #                       ErrorSeverity: {ErrorSeverity} \
    #                       ErrorTitle: {ErrorTitle} \
    #                       ErrorType: {ErrorType} \
    #                       PackagePushJobId: {PackagePushJobId}'.format(**contact))