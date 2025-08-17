import azure.functions as func
import datetime
import json
import logging

app = func.FunctionApp()

# Add a simple HTTP trigger for testing
@app.route(route="hello")
def http_trigger_hello(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    return func.HttpResponse(
        "Hello, world!",
        status_code=200
    )

from receipt_processor import blueprint as receipt_blueprint
app.register_blueprint(receipt_blueprint)