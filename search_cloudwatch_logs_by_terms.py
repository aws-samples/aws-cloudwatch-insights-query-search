import argparse
import json
import logging
import time

import boto3
import yaml

logger = logging.getLogger('query_cloudwatch_logs_by_term')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

_MIN = 1000
_HOUR= 60*1000
_DAY = 86400

LOGGABLE_RESOURCES = [ "AWS::Lambda::Function", "AWS::Logs::LogGroup" ]
LOG_QUERY_RANGE = {
    "MIN": _MIN,
    "HOUR": _HOUR,
    "DAY": _DAY
}

# boto3 clients
cfn_client = boto3.client('cloudformation')
logs_client = boto3.client('logs')


def get_stacks_for_partial_stack_name(partial_stack_name: str) -> list[str]:
    """
    Gets a list of stacks that include a partial stack name.

    Parameters:
    partial_stack_name (str): The partial name to be matched

    Returns:
    list[str]: List of Cloudformation stack names that include the partial stack name
    """
    matching_stacks = []
    paginator = cfn_client.get_paginator('list_stacks')
    response_iterator = paginator.paginate(
        StackStatusFilter=[
            'CREATE_COMPLETE',
            'UPDATE_COMPLETE'
        ]
    )
    for page in response_iterator:
        for stack_summary in page['StackSummaries']:
            if partial_stack_name in stack_summary['StackName']:
                matching_stacks.append(stack_summary['StackName'])

    return matching_stacks


def list_cloudformation_resources(stack_name: str) -> list[dict]:
    """
    Lists 'loggable' resources belonging to a Cloudformation stack.

    A 'loggable' resource is one that is defined in the LOGGABLE_RESOURCES list.

    Parameters:
    stack_name (str): The Cloudformation stack name

    Returns:
    list[dict]: List of 'loggable' resources belonging to the Cloudformation stack
    """
    response = cfn_client.describe_stack_resources(
        StackName=stack_name
    )
    logging_resources = [] 
    for stack_resource in response.get('StackResources',[]):
        # remove non castable items 
        if 'Timestamp' in stack_resource:
            del stack_resource['Timestamp']
        if 'DriftInformation' in stack_resource:
            del stack_resource['DriftInformation']

        # only include valid resources 
        if stack_resource["ResourceType"] in LOGGABLE_RESOURCES:
            logging_resources.append(stack_resource)

    return logging_resources
        

def _get_lambda_log_group_name(stack_resource: dict) -> str:
    """
    Gets the CloudWatch Log Group associated with a Lambda function

    Parameters:
    stack_resource (str): The Cloudformation stack resource

    Returns:
    str: The CloudWatch Log Group associated with a Lambda function
    """
    return f"/aws/lambda/{stack_resource['PhysicalResourceId']}"


def get_log_group_names(logging_resources: list[dict]) -> list[str]:
    """
    Gets the CloudWatch log group name associated with Cloudformation resources

    Parameters:
    logging_resources (list[dict]): List of  Cloudformation resources

    Returns:
    str: The CloudWatch Log Group names associated with loggable resources from the Cloudformation stack
    """
    log_group_names =[]
    for logging_resource in logging_resources:
        if  logging_resource["ResourceType"] == "AWS::Logs::LogGroup":
            log_group_names.append(logging_resource['PhysicalResourceId'])
        if  logging_resource["ResourceType"] == "AWS::Lambda::Function":
            log_group_names.append(_get_lambda_log_group_name(logging_resource))
        if logging_resource["ResourceType"] not in LOGGABLE_RESOURCES:
            raise ValueError(
                str(
                    f"ResourceType: {logging_resource['ResourceType']} does not match " +
                    f"one of the expected types: {','.join(LOGGABLE_RESOURCES)}."
                )
            )

    return log_group_names


def _get_query_time_range(start: int, end: int) -> tuple:
    """
    Gets a start and end epoch time that establishes a CloudWatch log group time range

    Parameters:
    start (int):    The start time of the time frame to search the CloudWatch logs
    end (int):      The end time of the time frame to search the CloudWatch logs

    Returns:
    tuple: The start and end time to search the CloudWatch logs
    """
    end_time = end
    start_time = end_time - start
    return start_time, end_time


def _get_cloudwatch_insights_query_string(query_terms: list[str], query_limit: int) -> str:
    """
    Gets the CloudWatch insights filter to be used to query for terms

    Parameters:
    query_terms (list[str]):    The query terms to be searched
    query_limit (int):          The limit to the number of results queried

    Returns:
    str: CloudWatch insights filter query containing the query terms to be searched
    """
    filter_concat_all_but_last = " ".join([f"@message like '{query_term}' or " for query_term in query_terms[:-1]])
    filter_concat = f"{filter_concat_all_but_last}{query_terms[-1]}"
    insights_query = (
        'fields @timestamp, @message ' +
        '| sort @timestamp desc ' +
        f'| filter ({filter_concat}) ' +
        f'| limit {query_limit}'
    )
    return insights_query


def _start_query_with_query_string(
        log_group_name: str,
        query_terms: list[str],
        query_limit: int,
        start_time: int,
        end_time: int
    ) -> dict:
    """
    Starts a CloudWatch insights query

    Parameters:
    log_group_name (str):       The log group name to be queried
    query_terms (list[str]):    The query terms to be searched
    query_limit (int):          The limit to the number of results queried
    start_time: (int):          Epoch time representing the query period start time
    end_time (int):             Epoch time representing the query period end time

    Returns:
    dict: Dict containing the log group name and the query id
    """   
    start_time, end_time = _get_query_time_range(start=start_time, end=end_time)
    try:
        response = logs_client.start_query(
            logGroupName=log_group_name,
            startTime=start_time,
            endTime=end_time,
            queryString=_get_cloudwatch_insights_query_string(
                query_terms=query_terms, 
                query_limit=query_limit
            ),
            limit=query_limit
        )
        return {
            "log_group_name": log_group_name,
            "query_id": response['queryId']
        }   
    except logs_client.exceptions.ResourceNotFoundException:
       logger.error("Log group %s not found. This normally means the log group has no log stream.", log_group_name)

    return {}


def query_log_files_for_terms(
        log_group_names: list[str],
        query_limit: int,
        start_time: int,
        end_time: int
    ) -> list[dict]:
    """
    Queries CloudWatch log groups for query terms that are defined in query_terms.yaml

    Parameters:
    log_group_names (list[str]):    The log group names to be queried
    query_limit (int):              The limit to the number of results queried
    start_time (int):               Epoch time representing the query period start time
    end_time (int):                 Epoch time representing the query period end time

    Returns:
    list[dict]: List of Dict containing the log group name and the query id
    """
    query_ids = [] 
    for log_group_name in log_group_names:
        _query_id = _start_query_with_query_string(
            log_group_name=log_group_name,
            query_terms=get_query_terms(),
            query_limit=query_limit,
            start_time=start_time,
            end_time=end_time
        )
        query_ids.append(_query_id)

    return query_ids


def get_query_results(query_ids: list[dict]) -> list[dict]:
    """
    Gets the results of a CloudWatch insights query

    Parameters:
    query_ids (list[dict]): A list of query ids from which to obtain search results

    Returns:
    list[dict]: List of Dict containing the search results
    """
    query_results =[]
    for query_id in query_ids:
        if 'query_id' in query_id:
            response = logs_client.get_query_results(
                queryId=query_id['query_id']
            )
            if len(response.get('results', [])) > 0:
                query_results.append(
                    { 
                        "log_group_name": query_id['log_group_name'],
                        "results": response.get('results')
                    } 
                )

    return query_results


def progress(percent=0, width=40) -> None:
    """
    Displays a progress bar in the console

    Parameters:
    percent (int):  The percentage of completed progress
    width (int):    The width of the progress bar

    Returns:
    None
    """
    left = width * percent // 100
    right = width - left

    tags = "#" * left
    spaces = " " * right

    print("\r", tags, spaces, sep="", end="", flush=True)


def query_loggable_resources(
        stack_name: str, 
        query_wait: int,
        query_limit: int,
        start_time: int,
        end_time: int
    ) -> None:
    """
    Queries the loggable resources of a Cloudformation stack for query terms
    over a specific time range

    Parameters:
    stack_name (str):   The name of the Cloudformation stack
    query_wait (int):   The width of the progress bar
    query_limit (int):  The limit to the number of results queried
    start_time (int):   Epoch time representing the query period start time
    end_time (int):     Epoch time representing the query period end time

    Returns:
    None
    """
    loggable_resources = list_cloudformation_resources(stack_name=stack_name)
    logger.info(
        "Found %d loggable_resources for Cloudformation stack: %s.", 
        len(loggable_resources),
        stack_name
    )

    if len(loggable_resources) == 0:
        logger.info("No log groups to query for for Cloudformation stack: %s.", stack_name)
        return None

    log_group_names = get_log_group_names(logging_resources=loggable_resources)

    query_ids = query_log_files_for_terms(
        log_group_names=log_group_names,
        query_limit=query_limit,
        start_time=start_time,
        end_time=end_time
    )
    logger.info("Starting queries.")

    logger.info("Waiting %d seconds before getting the query results.", query_wait)
    for i in range(query_wait):
        percent = int((i / query_wait) * 100)
        progress(percent)
        time.sleep(1)

    print("")
    logger.info("Getting the query results")
    query_results = get_query_results(query_ids=query_ids)
    
    if len(query_results) > 0:
        logger.warning("Query term match has been found in %d log groups.", len(query_results))
        logger.warning("Affected log groups are:")
        for log_group_result in query_results:
            logger.warning(log_group_result['log_group_name'])
        with open(file=f"{stack_name}_results.json",  mode="w", encoding="utf-8") as outfile:
            outfile.write(json.dumps(query_results, indent=2))
            logger.info(
                "Results file for stack %s written to %s_results.json", 
                stack_name, 
                stack_name
            )
    else:
        logger.info("No query terms match after checking %d log groups", len(log_group_names))
        logger.info("The following log groups were checked:")
        for log_group_result in query_results:
            logger.info(log_group_result['log_group_name'])


def _validate_args(_args: list) -> int:
    """
    Validates the CLI arguments

    Parameters:
    _args (list):   The command line arguments

    Returns:
    int: The start time of the range period to be searched
    """
    if _args.partialStackName:
        if _args.stackName:
            raise ValueError(
                "The partialStackName argument has been added. " +
                "Please do not supply a stackName."
            )

    if _args.stackName:
        if _args.partialStackName:
            raise ValueError(
                "The partialStackName argument has been added. " +
                "Please do not supply a stackName."
            )
    start_time = 0

    if _args.startTimeMins is not None:
        if _args.startTimeHours is not None and _args.startTimeDays is not None:
            raise ValueError(
                "Multiple startTime types are not supported. " +
                "Please select only one type of startTime."
            )
        start_time = _args.startTimeMins * LOG_QUERY_RANGE['MIN']

    if _args.startTimeHours is not None:
        if _args.startTimeMins is not None and _args.startTimeDays is not None:
            raise ValueError(
                "Multiple startTime types are not supported. " +
                "Please select only one type of startTime."
            )
        start_time = _args.startTimeHours * LOG_QUERY_RANGE['HOUR']

    if _args.startTimeDays is not None:
        if _args.startTimeMins is not None and _args.startTimeHours is not None:
            raise ValueError(
                "Multiple startTime types are not supported. " +
                "Please select only one type of startTime."
            )
        start_time = _args.startTimeDays * LOG_QUERY_RANGE['DAY']

    return start_time


def get_query_terms() -> list[str]:
    """
    Gets the terms to be queried during the CloudWatch Log Group search.

    The terms are read from the query_terms.yaml file.
    
    Parameters:
    None
  
    Returns:
    list[str]: The query terms
    """
    with open(file="query_terms.yaml", mode="r", encoding="utf-8") as stream:
        try:
            return yaml.safe_load(stream)['query-terms']
        except yaml.YAMLError as exc:
            ValueError(exc)


def main(_args: list, start_time: int) -> None:
    """
    Orchestrates the search for query terms contained in CloudWatch Log Groups
    associated with a Cloudformation stack over a defined time range.

    Parameters:
    _args (list):        The command line arguments
    start_time (int):   Epoch time representing the query period start time

    Returns:
    None
    """
    logger.info("Query terms; %s.", ', '.join(get_query_terms()))

    if _args.partialStackName:
        matching_stacks = get_stacks_for_partial_stack_name(partial_stack_name=_args.partialStackName)

        for matching_stack in matching_stacks:
            query_loggable_resources(
                stack_name=matching_stack,
                query_wait=_args.queryWait,
                query_limit=_args.queryLimit,
                start_time=start_time,
                end_time=_args.endTime
            )

        return None

    if _args.stackName:
        query_loggable_resources(
            stack_name=_args.stackName,
            query_wait=_args.queryWait,
            query_limit=_args.queryLimit,
            start_time=start_time,
            end_time=_args.endTime
        )

        return None


# Argument parsing
_parser = argparse.ArgumentParser(
    description='Check a CloudFormation stacks CloudWatch logs for query terms.'
)

# Add the partialStackName argument
_parser.add_argument(
    '--partialStackName',
    type=str,
    required=False,
    help='CloudFormation stacks which include the partial name'
)

# Add the stackName argument
_parser.add_argument(
    '--stackName',
    type=str,
    required=False,
    help='the CloudFormation stack name'
)

# Add the queryWait argument
_parser.add_argument(
    '--queryWait',
    type=int,
    required=False,
    default=60,
    help='how many seconds to wait for the queries to complete'
)

# Add the queryLimit argument
_parser.add_argument(
    '--queryLimit',
    type=int,
    required=False,
    default=1000,
    help='the number of results to return per query'
)

# Add the startTimeMins argument
_parser.add_argument(
    '--startTimeMins',
    type=int,
    required=False,
    help='the start time, in minutes, for which the logs are to queried'
)

# Add the startTimeHours argument
_parser.add_argument(
    '--startTimeHours',
    type=int,
    required=False,
    help='the start time, in hours, for which the logs are to queried'
)

# Add the startTimeDays argument
_parser.add_argument(
    '--startTimeDays',
    type=int,
    required=False,
    help='the start time, in days, for which the logs are to queried'
)

# Add the endTime argument
_parser.add_argument(
    '--endTime',
    type=int,
    required=False,
    default=int(time.time()),
    help='the end time for which the logs are to queried.'
)

args = _parser.parse_args()
main(_args=args, start_time=_validate_args(args))
