import os
import findspark
import requests
from bs4 import BeautifulSoup
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

findspark.init()

# Initialize Spark session
spark = SparkSession.builder.appName("GitHubPRData").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)  # Property used to format output tables better


# Step 1: Extract - Get pull requests data and save to JSON files
def scrape_repo_info(html_content):
    soup = BeautifulSoup(html_content[0].text, 'html.parser')
    repo_list = []

    for repo in soup.find_all('li', class_='Box-row'):
        repo_info = {
            'repository_name': repo.find('a', itemprop='name codeRepository').get_text(strip=True),
            'repository_id': 'https://github.com' + repo.find('a', itemprop='name codeRepository')['href'],
            'Organization Name': html_content[1].split('/')[-1],
            'num_prs': '',
            'num_prs_merged': '',
            'merged_at': '',
            'repository_owner': '',
            'is_compliant': ''
        }
        repo_list.append(repo_info)

    return repo_list


def main(github_url):
    response_repos = requests.get(f'{github_url}/repositories')
    response_repos = (response_repos, github_url)

    if response_repos[0].status_code == 200:
        # Use map to apply the scraping logic to each repository HTML content
        repo_info_list = list(
            filter(None, spark.sparkContext.parallelize([response_repos]).map(scrape_repo_info).collect()))

        # Save pull request data to JSON files
        for idx, repo_info in enumerate(repo_info_list):
            spark.createDataFrame(repo_info).write.json(f"extracted_data/repo_{idx}.json", mode='overwrite')
    else:
        print(f"Failed to fetch repository data. Status code: {response_repos.status_code}")


    # Define schema for the table
    schema = StructType([
        StructField("Organization_Name", StringType(), True),
        StructField("repository_id", StringType(), True),
        StructField("repository_name", StringType(), True),
        StructField("repository_owner", StringType(), True),
        StructField("num_prs", IntegerType(), True),
        StructField("num_prs_merged", IntegerType(), True),
        StructField("merged_at", StringType(), True),
        StructField("is_compliant", StringType(), True),
    ])

    # Read JSON files and create DataFrame with the specified schema
    transformed_data = spark.read.schema(schema).json("extracted_data/*.json")

    # Save transformed data to Parquet file
    transformed_data.write.parquet("transformed_data.parquet", mode='overwrite')

    return True


if __name__ == '__main__':
    github_url = 'https://github.com/orgs/Scytale-exercise'

    # call main
    main(github_url)

    # Read from the Parquet file
    transformed_data_read = spark.read.parquet("transformed_data.parquet")
    transformed_data_read.show()

    # Stop Spark session
    spark.stop()

