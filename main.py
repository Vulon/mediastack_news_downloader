from google.cloud import bigquery
from google.cloud import pubsub_v1
import google.cloud.logging
import requests
import datetime
import logging
import argparse
from utils import get_mediastack_api_key
from config import load_config


gc_logger = google.cloud.logging.Client()
gc_logger.setup_logging()
import logging


def main(*args, **kwargs):
    """
    Entry point.
    Downloads news from the REST API and stores them in the BigQuery table.
    :param args:
    :param kwargs:
    :return:
    """
    logging.info("Mediastack download cron started")
    cfg = load_config()
    parser = argparse.ArgumentParser(description="Scheduled mediastack news donwloader")

    parser.add_argument('-pr', '--project_id', type=str, help='A project id for BigQuery table', default=cfg.project_id)
    parser.add_argument('-d', '--dataset_id', type=str, help='A dataset id for BigQuery table', default=cfg.dataset_id)
    parser.add_argument('-t', '--table_id', type=str, help='A table name for BigQuery', default=cfg.table_id)
    parser.add_argument('-pg', '--page_count', type=int, help='A number of pages to download per session', default=cfg.page_count)
    parser.add_argument('-pt', '--publish_topic', type=str, help='A name of the topic to publish to', default=cfg.output_topic)
    cli_args = parser.parse_args()

    full_table_name = "{}.{}.{}".format(cli_args.project_id, cli_args.dataset_id, cli_args.table_id)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(cli_args.project_id, cli_args.publish_topic)

    api_key = get_mediastack_api_key(cfg.api_key_filename, cfg.api_key_bucket_name)
    url = cfg.url_base
    date = datetime.datetime.now() - datetime.timedelta(1)
    date = date.strftime("%Y-%m-%d")
    offset = 0
    client = bigquery.Client()
    table = client.get_table(full_table_name)

    query_job = client.query(
        f"""
        SELECT
        distinct url
        FROM `{full_table_name}`"""
    )
    distinct_urls = query_job.result()
    distinct_urls = { row.url for row in distinct_urls }

    for i in range(cli_args.page_count):
        print("Loop", i)
        params = {
            "access_key" : api_key['key'],
            "languages" : "en",
            "limit" : cfg.page_limit,
            "sort" : "published_desc",
            "offset" : i * cfg.page_limit,
            "date" : date
        }
        try:
            r = requests.get(
                url,
                params = params
            )
            pagination = r.json()['pagination']
            offset += pagination['count']

            entries_list = r.json()['data']
            rows_to_insert = []
            for entry in entries_list:
                if entry["url"] in distinct_urls:
                    continue
                date = datetime.datetime.strptime(entry["published_at"], '%Y-%m-%dT%H:%M:%S%z')
                date = date.strftime('%Y-%m-%d %H:%M:%S.%f')
                rows_to_insert.append({
                    "url": entry["url"],
                    "title": entry["title"],
                    "description": entry["description"],
                    "source": entry["source"],
                    "category": entry["category"],
                    "country": entry["country"],
                    "date": date,
                    "processed_flag": False
                })
            if len(rows_to_insert) < 1:
                logging.warning("No new rows to insert")
                continue
            errors = client.insert_rows_json(table, rows_to_insert)
            logging.info(f"Writing {len(rows_to_insert)} rows to table")
            # print(f"Writing {len(rows_to_insert)} rows to table")

            for e in errors:
                logging.error(e)

            logging.info(f"Read {len(entries_list)} entries")

        except Exception as e:
            logging.exception(e)

    future = publisher.publish(topic_path, b'Mediastack')
    result = future.result()
    logging.info(result)

if __name__ == "__main__":
    main()