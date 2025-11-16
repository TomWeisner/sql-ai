### Bedrock

This folder wraps the AWS Bedrock runtime: models, body builders, helpers, and the thin
service class that actually makes API calls.  
Nothing here knows about SQL or Athena—it only knows how to invoke a model with a prompt.  
If you need to add a model or tweak invocation parameters, this is where you do it.
