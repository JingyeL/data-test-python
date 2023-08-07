"""
As a Data Engineer at a rapidly growing e-commerce company, you are given two CSV files: users.csv and purchases.csv.
The users.csv file contains details about your users, and the purchases.csv file holds records of all purchases made.

Your team is interested in gaining a deeper understanding of customer behavior.
A question they’re particularly interested in is: "What is the total spending of each customer?"

Your task is to extract meaningful information from these data sets to answer this question.
The output of your work should be a JSON file, output.json.
"""


def data_join(df_user, df_purchase, output):
    df = df_user.join(df_purchase, "user_id")
    df = df.groupBy("user_id", "name").sum("price")
    df = df.withColumnRenamed("sum(price)", "total_amount")
    df = df.drop("price")
    df.write.json(output, mode="overwrite")
