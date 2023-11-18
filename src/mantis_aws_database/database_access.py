from __future__ import annotations
import boto3
import json
import os
import logging

# logging.basicConfig(format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S")
# project_attributes = {
#     "PK": "S",
#     "SK": "S",
#     "Project_Name": "S",
#     "Project_Info": "S",
#     "Project_Region": "S",
#     "Project_Country": "S",
# }

# layer_attributes = {
#     "PK": "S",
#     "SK": "S",
#     "Top_Depth": "N",
#     "Vp": "N",
#     "Vs": "N",
#     "Rho": "N",
# }


# def _create_item(item_type: dict, attributes: dict) -> dict:
#     """Creates a dictionary of attributes for a DynamoDB item.

#     :param item: A dictionary of attributes for the item.
#     :param attributes: A dictionary of attributes for the item.
#     :return: The item dictionary with attributes added.
#     """
#     item["Attributes"] = attributes
#     return item


class DatabaseAccess:
    # instead of a schema so that we keep track of database attributes associated with a project
    # and layers and can migrate existing projects to whatever new scheme we decide in the future
    """Encapsulates an Amazon DynamoDB table for project methods."""

    def __init__(self, userID: str, table_name: str = None):
        # The table variable is set during the scenario in the call to
        # 'exists' if the table exists. Otherwise, it is set by 'create_table'.

        self.client = boto3.client("dynamodb")
        self.userID = userID
        self.table = None
        try:
            self.table = self.resource.Table(table_name)
        except self.client.exceptions.ResourceNotFoundException as e:
            logging.error("Exception occurred", exc_info=True)
            pass

    def _create_hash_secondary(
        self, user_id: str | None, project_id: str | None, layer_id: str | None
    ) -> tuple:
        # """Creates a hash key for a primary and secondary index."""
        # hashkey = ""
        # sortkey = ""
        # if user_id is None:
        #     raise ValueError("userID cannot be None")
        # if project_id is None:
        #     hash = user_id
        #     sort = project_id
        # else:
        #     hash = user_id + "#" + project_id
        #     if layer_id is None:
        #         sort = project_id
        #     else:
        #         sort = project_id + "#" + layer_id
        # return userID + "#" + project_id
        ...

    def _delByID(self, projectId):
        hash = self.userID + "#" + projectId
        pass
        # try:
        #     self.table.delete_item(Key={"PK": projectId, "title": title})
        # except self.client.ClientError as err:
        #     logger.error(
        #         "Couldn't delete movie %s. Here's why: %s: %s",
        #         title,
        #         err.response["Error"]["Code"],
        #         err.response["Error"]["Message"],
        #     )
        #     raise

    def _createByID(self, id):
        pass

    def _amendByID(self, project_id):
        pass

    def _getByID(self, project_id):
        pass

    def delete_project_by_ID(self, project_id):
        """
        Deletes a movie only if it is rated below a specified value. By using a
        condition expression in a delete operation, you can specify that an item is
        deleted only when it meets certain criteria.

        :param title: The title of the movie to delete.
        :param year: The release year of the movie to delete.
        :param rating: The rating threshold to check before deleting the movie.
        """
        try:
            self.table.delete_item(
                Key={"year": year, "title": title},
                ConditionExpression="info.rating <= :val",
                ExpressionAttributeValues={":val": Decimal(str(rating))},
            )
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(
                    "Didn't delete %s because its rating is greater than %s.",
                    title,
                    rating,
                )
            else:
                logger.error(
                    "Couldn't delete movie %s. Here's why: %s: %s",
                    title,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            raise
